// This file contains the model router and model routing rule endpoints. A
// model router classifies a request into a complexity tier; a model routing
// rule, matched after the routing rules, names the router and the model each
// tier gets.

package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/fasthttp/router"
	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/plugins/routing/complexity"
	"github.com/maximhq/bifrost/plugins/routing/complexity/suggest"
	"github.com/maximhq/bifrost/plugins/routing/rules"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

// ModelRoutingTargetRequest is one model for one tier of a model routing rule.
type ModelRoutingTargetRequest struct {
	Tier          string                              `json:"tier"`
	Provider      *string                             `json:"provider,omitempty"`
	Model         *string                             `json:"model,omitempty"`
	KeyID         *string                             `json:"key_id,omitempty"`
	Weight        float64                             `json:"weight"`
	TTFTTimeoutMs *int                                `json:"ttft_timeout_ms,omitempty"`
	Fallbacks     []configstoreTables.RoutingFallback `json:"fallbacks,omitempty"`
}

// ModelRoutingRuleRequest is the body that creates or replaces a model routing rule.
type ModelRoutingRuleRequest struct {
	Name          string                      `json:"name"`
	Description   string                      `json:"description,omitempty"`
	Enabled       *bool                       `json:"enabled,omitempty"` // nil = true on create, unchanged on update
	CelExpression string                      `json:"cel_expression,omitempty"`
	Query         map[string]any              `json:"query,omitempty"` // the dashboard's condition builder state; never evaluated
	ModelRouterID string                      `json:"model_router_id"`
	Scope         string                      `json:"scope,omitempty"` // defaults to "global"
	ScopeID       *string                     `json:"scope_id,omitempty"`
	Priority      int                         `json:"priority,omitempty"`
	Targets       []ModelRoutingTargetRequest `json:"targets"`
}

// registerModelRouterRoutes registers the model router and model routing rule
// routes. They never shipped under /api/governance, so they have no legacy
// alias.
func (h *RoutingHandler) registerModelRouterRoutes(r *router.Router, middlewares ...schemas.BifrostHTTPMiddleware) {
	handle := func(method, path string, handler fasthttp.RequestHandler) {
		r.Handle(method, path, lib.ChainMiddlewares(handler, middlewares...))
	}
	handle(fasthttp.MethodGet, "/api/routing/model-routers", h.listModelRouters)
	handle(fasthttp.MethodPost, "/api/routing/model-routers", h.createModelRouter)
	handle(fasthttp.MethodGet, "/api/routing/model-routers/{router_id}", h.getModelRouter)
	handle(fasthttp.MethodPut, "/api/routing/model-routers/{router_id}", h.updateModelRouter)
	handle(fasthttp.MethodDelete, "/api/routing/model-routers/{router_id}", h.deleteModelRouter)
	handle(fasthttp.MethodPost, "/api/routing/model-routers/{router_id}/reset", h.resetModelRouter)
	handle(fasthttp.MethodGet, "/api/routing/model-routers/{router_id}/status", h.getModelRouterStatus)
	handle(fasthttp.MethodPost, "/api/routing/model-routers/{router_id}/status/retry", h.retryModelRouterWarmup)
	handle(fasthttp.MethodPost, "/api/routing/model-routers/{router_id}/classify", h.classifyWithModelRouter)

	handle(fasthttp.MethodGet, "/api/routing/model-routing-rules", h.listModelRoutingRules)
	handle(fasthttp.MethodPost, "/api/routing/model-routing-rules", h.createModelRoutingRule)
	handle(fasthttp.MethodGet, "/api/routing/model-routing-rules/{rule_id}", h.getModelRoutingRule)
	handle(fasthttp.MethodPut, "/api/routing/model-routing-rules/{rule_id}", h.updateModelRoutingRule)
	handle(fasthttp.MethodDelete, "/api/routing/model-routing-rules/{rule_id}", h.deleteModelRoutingRule)
}

// pathID reads a route parameter.
func pathID(ctx *fasthttp.RequestCtx, name string) string {
	id, _ := ctx.UserValue(name).(string)
	return strings.TrimSpace(id)
}

// DefaultModelRouter is the default router as served before its first
// save, or when its stored config cannot be read: the built-in defaults.
func DefaultModelRouter() configstore.ModelRouter {
	return configstore.ModelRouter{
		ID:                       configstore.DefaultModelRouterID,
		Name:                     configstore.DefaultModelRouterName,
		ComplexityAnalyzerConfig: complexity.DefaultAnalyzerConfig(),
	}
}

// loadModelRouter reads one router for the API. The default router reads as
// the built-in defaults until it is saved, and when its row cannot be read,
// matching what it serves.
func (h *RoutingHandler) loadModelRouter(ctx *fasthttp.RequestCtx, id string) (*configstore.ModelRouter, error) {
	router, err := h.configStore.GetModelRouter(ctx, id)
	if err == nil {
		return router, nil
	}
	if id == configstore.DefaultModelRouterID && (errors.Is(err, configstore.ErrNotFound) || errors.Is(err, configstore.ErrConfigUnreadable)) {
		view := DefaultModelRouter()
		return &view, nil
	}
	return nil, err
}

// sendModelRouterLoadError maps a failed router read to its status.
func sendModelRouterLoadError(ctx *fasthttp.RequestCtx, id string, err error) {
	switch {
	case errors.Is(err, configstore.ErrNotFound):
		SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
	case errors.Is(err, configstore.ErrConfigUnreadable):
		SendError(ctx, fasthttp.StatusConflict, fmt.Sprintf("model router %q is stored in a form this version cannot read; save or reset it: %v", id, err))
	default:
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to get model router: %v", err))
	}
}

// decodeModelRouter reads a router body strictly, like the complexity config
// endpoint, so a misspelled setting is reported instead of dropped.
func decodeModelRouter(ctx *fasthttp.RequestCtx) (*configstore.ModelRouter, bool) {
	var payload configstore.ModelRouter
	if !decodeStrictJSON(ctx, &payload) {
		return nil, false
	}
	return &payload, true
}

// decodeStrictJSON decodes the body into payload, refusing unknown fields and
// trailing values so a misspelled setting is an error rather than dropped. It
// answers 400 and returns false on failure.
func decodeStrictJSON(ctx *fasthttp.RequestCtx, payload any) bool {
	decoder := json.NewDecoder(bytes.NewReader(ctx.PostBody()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(payload); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, fmt.Sprintf("Invalid request payload: %v", err))
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		SendError(ctx, fasthttp.StatusBadRequest, "invalid request format: multiple JSON values")
		return false
	}
	return true
}

// prepareModelRouter validates a router body for saving: its identity, its
// config, and the runtime dependencies that config needs.
func (h *RoutingHandler) prepareModelRouter(ctx *fasthttp.RequestCtx, router *configstore.ModelRouter) bool {
	if err := configstore.ValidateModelRouterIdentity(router); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return false
	}
	normalized, err := complexity.ValidateAndNormalize(&router.ComplexityAnalyzerConfig)
	if err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return false
	}
	if err := h.routingManager.ValidateComplexityAnalyzerConfig(ctx, normalized); err != nil {
		if errors.Is(err, ErrRoutingPluginUnavailable) {
			SendError(ctx, fasthttp.StatusInternalServerError, err.Error())
			return false
		}
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return false
	}
	router.ComplexityAnalyzerConfig = *normalized
	return true
}

// reloadModelRouter serves a saved router, reporting a failure the way the
// other routing endpoints do.
func (h *RoutingHandler) reloadModelRouter(ctx *fasthttp.RequestCtx, id string) bool {
	if err := h.routingManager.ReloadModelRouter(ctx, id); err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to reload model router in memory: %v, please restart bifrost to sync with the database", err))
		return false
	}
	return true
}

// listModelRouters returns every router, the default first.
func (h *RoutingHandler) listModelRouters(ctx *fasthttp.RequestCtx) {
	stored, err := h.configStore.GetModelRouters(ctx)
	if err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to list model routers: %v", err))
		return
	}
	routers := make([]configstore.ModelRouter, 0, len(stored)+1)
	if len(stored) == 0 || stored[0].ID != configstore.DefaultModelRouterID {
		routers = append(routers, DefaultModelRouter())
	}
	routers = append(routers, stored...)
	SendJSON(ctx, map[string]any{"routers": routers, "count": len(routers)})
}

// getModelRouter returns one router.
func (h *RoutingHandler) getModelRouter(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	router, err := h.loadModelRouter(ctx, id)
	if err != nil {
		sendModelRouterLoadError(ctx, id, err)
		return
	}
	SendJSON(ctx, router)
}

// createModelRouter stores and serves a new router. An omitted id is generated.
func (h *RoutingHandler) createModelRouter(ctx *fasthttp.RequestCtx) {
	router, ok := decodeModelRouter(ctx)
	if !ok {
		return
	}
	if strings.TrimSpace(router.ID) == "" {
		router.ID = uuid.NewString()
	}
	if !h.prepareModelRouter(ctx, router) {
		return
	}
	if err := h.configStore.CreateModelRouter(ctx, router); err != nil {
		switch {
		case errors.Is(err, configstore.ErrAlreadyExists):
			SendError(ctx, fasthttp.StatusConflict, err.Error())
		case errors.Is(err, configstore.ErrDefaultModelRouter):
			SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		default:
			SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to create model router: %v", err))
		}
		return
	}
	if !h.reloadModelRouter(ctx, router.ID) {
		return
	}
	saved, err := h.loadModelRouter(ctx, router.ID)
	if err != nil {
		sendModelRouterLoadError(ctx, router.ID, err)
		return
	}
	lib.RecordAuditChange(ctx, "model_router.created", "model_router", saved.ID, saved.Name, nil, saved)
	SendJSON(ctx, saved)
}

// updateModelRouter replaces a router's name, description and config. The
// default router is created by its first update.
func (h *RoutingHandler) updateModelRouter(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	router, ok := decodeModelRouter(ctx)
	if !ok {
		return
	}
	router.ID = id
	if !h.prepareModelRouter(ctx, router) {
		return
	}
	before, _ := h.loadModelRouter(ctx, id)
	if err := h.configStore.UpdateModelRouter(ctx, router); err != nil {
		switch {
		case errors.Is(err, configstore.ErrNotFound):
			SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
		case errors.Is(err, configstore.ErrAlreadyExists):
			SendError(ctx, fasthttp.StatusConflict, err.Error())
		default:
			SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to update model router: %v", err))
		}
		return
	}
	if !h.reloadModelRouter(ctx, id) {
		return
	}
	saved, err := h.loadModelRouter(ctx, id)
	if err != nil {
		sendModelRouterLoadError(ctx, id, err)
		return
	}
	lib.RecordAuditChange(ctx, "model_router.updated", "model_router", saved.ID, saved.Name, before, saved)
	SendJSON(ctx, saved)
}

// deleteModelRouter removes a router. The default router cannot be deleted,
// and neither can one a model routing rule names.
func (h *RoutingHandler) deleteModelRouter(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	before, _ := h.configStore.GetModelRouter(ctx, id)
	if err := h.configStore.DeleteModelRouter(ctx, id); err != nil {
		switch {
		case errors.Is(err, configstore.ErrDefaultModelRouter):
			SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		case errors.Is(err, configstore.ErrModelRouterInUse):
			SendError(ctx, fasthttp.StatusConflict, err.Error())
		case errors.Is(err, configstore.ErrNotFound):
			SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
		default:
			SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to delete model router: %v", err))
		}
		return
	}
	if err := h.routingManager.RemoveModelRouter(ctx, id); err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("model router deleted but not removed from memory: %v, please restart bifrost to sync with the database", err))
		return
	}
	name := id
	if before != nil {
		name = before.Name
	}
	lib.RecordAuditChange(ctx, "model_router.deleted", "model_router", id, name, before, nil)
	SendJSON(ctx, map[string]any{"message": "Model router deleted successfully"})
}

// resetModelRouter restores a router's phrases to the built-in defaults and
// keeps its other settings.
func (h *RoutingHandler) resetModelRouter(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	before, _ := h.loadModelRouter(ctx, id)
	defaults := complexity.DefaultAnalyzerConfig()
	restored, err := h.configStore.ResetModelRouter(ctx, id, &defaults)
	if err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
			return
		}
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("failed to reset model router: %v", err))
		return
	}
	if !h.reloadModelRouter(ctx, id) {
		return
	}
	lib.RecordAuditChange(ctx, "model_router.reset", "model_router", restored.ID, restored.Name, before, restored)
	SendJSON(ctx, restored)
}

// getModelRouterStatus reports one router's classifier readiness, in the
// shape of the complexity analyzer status.
func (h *RoutingHandler) getModelRouterStatus(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	semantic, llm, err := h.routingManager.GetModelRouterStatus(ctx, id)
	if err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
			return
		}
		SendError(ctx, fasthttp.StatusServiceUnavailable, fmt.Sprintf("failed to get model router status: %v", err))
		return
	}
	SendJSON(ctx, complexityStatusResponse{
		SemanticStatusInfo:            semantic,
		LLM:                           &llm,
		DecisionDefaults:              configstore.DefaultComplexityDecisionGuidance(),
		SuggestionDefaultInstructions: suggest.DefaultInstructions(),
		LLMDefaultPrompt:              complexity.DefaultLLMClassifierGuidance(),
	})
}

// retryModelRouterWarmup restarts one router's failed semantic warmup.
func (h *RoutingHandler) retryModelRouterWarmup(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "router_id")
	status, retried, err := h.routingManager.RetryModelRouterWarmup(ctx, id)
	if err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, fmt.Sprintf("model router %q not found", id))
			return
		}
		SendError(ctx, fasthttp.StatusServiceUnavailable, fmt.Sprintf("failed to retry model router warmup: %v", err))
		return
	}
	if !retried {
		SendError(ctx, fasthttp.StatusConflict, "semantic complexity warmup can only be retried after a failure")
		return
	}
	lib.RecordAuditChange(ctx, "model_router.warmup_retried", "model_router", id, id, nil, map[string]any{"retried": true})
	SendJSONWithStatus(ctx, complexityStatusResponse{SemanticStatusInfo: status}, fasthttp.StatusAccepted)
}

// modelRoutingRuleFromRequest builds the stored rule a request describes.
func modelRoutingRuleFromRequest(id string, req *ModelRoutingRuleRequest) *configstoreTables.TableModelRoutingRule {
	rule := &configstoreTables.TableModelRoutingRule{
		ID:            id,
		Name:          req.Name,
		Description:   req.Description,
		Enabled:       req.Enabled,
		CelExpression: req.CelExpression,
		ParsedQuery:   req.Query,
		ModelRouterID: req.ModelRouterID,
		Scope:         req.Scope,
		ScopeID:       req.ScopeID,
		Priority:      req.Priority,
		Targets:       make([]configstoreTables.TableModelRoutingTarget, 0, len(req.Targets)),
	}
	for _, t := range req.Targets {
		rule.Targets = append(rule.Targets, configstoreTables.TableModelRoutingTarget{
			Tier:            t.Tier,
			Provider:        t.Provider,
			Model:           t.Model,
			KeyID:           t.KeyID,
			Weight:          t.Weight,
			TTFTTimeoutMs:   nilIfZero(t.TTFTTimeoutMs),
			ParsedFallbacks: t.Fallbacks,
		})
	}
	return rule
}

// prepareModelRoutingRule validates a rule for saving: its structure, its
// condition, its fallbacks and its scope's entity.
func (h *RoutingHandler) prepareModelRoutingRule(ctx *fasthttp.RequestCtx, rule *configstoreTables.TableModelRoutingRule) bool {
	if err := configstore.ValidateModelRoutingRule(rule); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return false
	}
	if err := rules.ValidateModelRuleCEL(rule.CelExpression); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, fmt.Sprintf("invalid CEL expression: %s", err.Error()))
		return false
	}
	for i, t := range rule.Targets {
		if err := validateRoutingFallbacks(t.ParsedFallbacks); err != nil {
			SendError(ctx, fasthttp.StatusBadRequest, fmt.Sprintf("targets[%d]: %s", i, err.Error()))
			return false
		}
	}
	if rule.ScopeID != nil {
		if err := h.validateRoutingScopeID(ctx, rule.Scope, *rule.ScopeID); err != nil {
			sendRoutingScopeIDValidationError(ctx, err)
			return false
		}
	}
	return true
}

// sendModelRoutingRuleSaveError maps a failed rule write to its status.
func sendModelRoutingRuleSaveError(ctx *fasthttp.RequestCtx, err error) {
	switch {
	case errors.Is(err, configstore.ErrModelRoutingRulePriorityTaken):
		SendError(ctx, fasthttp.StatusConflict, fmt.Sprintf("%v; use a different priority", err))
	case errors.Is(err, configstore.ErrAlreadyExists):
		SendError(ctx, fasthttp.StatusConflict, err.Error())
	case errors.Is(err, configstore.ErrUnknownModelRouter):
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
	case errors.Is(err, configstore.ErrNotFound):
		SendError(ctx, fasthttp.StatusNotFound, "Model routing rule not found")
	default:
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("Failed to save model routing rule: %v", err))
	}
}

// listModelRoutingRules returns every model routing rule, priority ascending.
func (h *RoutingHandler) listModelRoutingRules(ctx *fasthttp.RequestCtx) {
	stored, err := h.configStore.GetModelRoutingRules(ctx)
	if err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to get model routing rules")
		return
	}
	SendJSON(ctx, map[string]any{"rules": stored, "count": len(stored)})
}

// getModelRoutingRule returns one model routing rule.
func (h *RoutingHandler) getModelRoutingRule(ctx *fasthttp.RequestCtx) {
	rule, err := h.configStore.GetModelRoutingRule(ctx, pathID(ctx, "rule_id"))
	if err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, "Model routing rule not found")
			return
		}
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to retrieve model routing rule")
		return
	}
	SendJSON(ctx, rule)
}

// createModelRoutingRule stores and serves a new model routing rule.
func (h *RoutingHandler) createModelRoutingRule(ctx *fasthttp.RequestCtx) {
	var req ModelRoutingRuleRequest
	if err := sonic.Unmarshal(ctx.PostBody(), &req); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, "Invalid JSON")
		return
	}
	rule := modelRoutingRuleFromRequest(uuid.NewString(), &req)
	if !h.prepareModelRoutingRule(ctx, rule) {
		return
	}
	if err := h.configStore.CreateModelRoutingRule(ctx, rule); err != nil {
		sendModelRoutingRuleSaveError(ctx, err)
		return
	}
	if err := h.routingManager.ReloadModelRoutingRule(ctx, rule.ID); err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("Failed to reload model routing rule in memory: %v, please restart bifrost to sync with the database", err))
		return
	}
	saved, err := h.configStore.GetModelRoutingRule(ctx, rule.ID)
	if err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to retrieve model routing rule")
		return
	}
	lib.RecordAuditChange(ctx, "model_routing_rule.created", "model_routing_rule", saved.ID, saved.Name, nil, saved)
	SendJSON(ctx, saved)
}

// updateModelRoutingRule replaces a model routing rule and its targets.
func (h *RoutingHandler) updateModelRoutingRule(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "rule_id")
	var req ModelRoutingRuleRequest
	if err := sonic.Unmarshal(ctx.PostBody(), &req); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, "Invalid JSON")
		return
	}
	before, err := h.configStore.GetModelRoutingRule(ctx, id)
	if err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, "Model routing rule not found")
			return
		}
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to retrieve model routing rule")
		return
	}
	rule := modelRoutingRuleFromRequest(id, &req)
	// The config.json hash is kept, as routing rule edits keep theirs, so a
	// dashboard edit survives a reboot until the file's entry changes.
	rule.ConfigHash = before.ConfigHash
	if !h.prepareModelRoutingRule(ctx, rule) {
		return
	}
	if err := h.configStore.UpdateModelRoutingRule(ctx, rule); err != nil {
		sendModelRoutingRuleSaveError(ctx, err)
		return
	}
	if err := h.routingManager.ReloadModelRoutingRule(ctx, id); err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("Failed to reload model routing rule in memory: %v, please restart bifrost to sync with the database", err))
		return
	}
	saved, err := h.configStore.GetModelRoutingRule(ctx, id)
	if err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to retrieve model routing rule")
		return
	}
	lib.RecordAuditChange(ctx, "model_routing_rule.updated", "model_routing_rule", saved.ID, saved.Name, before, saved)
	SendJSON(ctx, saved)
}

// deleteModelRoutingRule removes a model routing rule.
func (h *RoutingHandler) deleteModelRoutingRule(ctx *fasthttp.RequestCtx) {
	id := pathID(ctx, "rule_id")
	before, _ := h.configStore.GetModelRoutingRule(ctx, id)
	if err := h.configStore.DeleteModelRoutingRule(ctx, id); err != nil {
		if errors.Is(err, configstore.ErrNotFound) {
			SendError(ctx, fasthttp.StatusNotFound, "Model routing rule not found")
			return
		}
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("Failed to delete model routing rule: %v", err))
		return
	}
	if err := h.routingManager.RemoveModelRoutingRule(ctx, id); err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, fmt.Sprintf("Model routing rule deleted but not removed from memory: %v, please restart bifrost to sync with the database", err))
		return
	}
	name := id
	if before != nil {
		name = before.Name
	}
	lib.RecordAuditChange(ctx, "model_routing_rule.deleted", "model_routing_rule", id, name, before, nil)
	SendJSON(ctx, map[string]any{"message": "Model routing rule deleted successfully"})
}
