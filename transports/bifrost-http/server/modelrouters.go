package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/plugins/routing"
	"github.com/maximhq/bifrost/plugins/routing/complexity"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/handlers"
)

// ReloadModelRouter serves a saved model router from the config store. The
// default router has no row until it is first saved, and then serves the
// built-in defaults.
func (s *BifrostHTTPServer) ReloadModelRouter(ctx context.Context, id string) error {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return fmt.Errorf("routing plugin not found: %w", err)
	}
	router, err := s.Config.ConfigStore.GetModelRouter(ctx, id)
	if errors.Is(err, configstore.ErrNotFound) && id == configstore.DefaultModelRouterID {
		defaults := handlers.DefaultModelRouter()
		router, err = &defaults, nil
	}
	if err != nil {
		return fmt.Errorf("failed to get model router from config store: %w", err)
	}
	return routingPlugin.ReloadModelRouter(router)
}

// RemoveModelRouter stops serving a deleted router and removes its phrase
// suggestions and runs, which belong to no router once it is gone. The
// router is already gone from the store and from memory when that cleanup
// runs, so a cleanup failure is logged rather than reported as a failed
// removal.
func (s *BifrostHTTPServer) RemoveModelRouter(ctx context.Context, id string) error {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return fmt.Errorf("routing plugin not found: %w", err)
	}
	if err := routingPlugin.RemoveModelRouter(id); err != nil {
		return err
	}
	if s.Config != nil && s.Config.LogsStore != nil {
		if store, ok := s.Config.LogsStore.(logstore.ComplexitySuggestionStore); ok {
			if err := store.DeleteComplexityRouterSuggestions(ctx, id); err != nil {
				logger.Warn("failed to delete phrase suggestions of removed model router %q: %v", id, err)
			}
		}
	}
	return nil
}

// ReloadModelRoutingRule serves a saved model routing rule from the config store.
func (s *BifrostHTTPServer) ReloadModelRoutingRule(ctx context.Context, id string) error {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return fmt.Errorf("routing plugin not found: %w", err)
	}
	rule, err := s.Config.ConfigStore.GetModelRoutingRule(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get model routing rule from config store: %w", err)
	}
	return routingPlugin.ReloadModelRoutingRule(rule)
}

// RemoveModelRoutingRule stops serving a deleted model routing rule.
func (s *BifrostHTTPServer) RemoveModelRoutingRule(_ context.Context, id string) error {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return fmt.Errorf("routing plugin not found: %w", err)
	}
	routingPlugin.RemoveModelRoutingRule(id)
	return nil
}

// GetModelRouterStatus reports one router's classifier readiness.
// configstore.ErrNotFound means the plugin does not serve the router.
func (s *BifrostHTTPServer) GetModelRouterStatus(_ context.Context, id string) (complexity.SemanticStatusInfo, complexity.LLMStatusInfo, error) {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return complexity.SemanticStatusInfo{}, complexity.LLMStatusInfo{}, fmt.Errorf("routing plugin not found: %w", err)
	}
	semantic, ok := routingPlugin.ModelRouterSemanticStatus(id)
	if !ok {
		return complexity.SemanticStatusInfo{}, complexity.LLMStatusInfo{}, configstore.ErrNotFound
	}
	llm, _ := routingPlugin.ModelRouterLLMStatus(id)
	return semantic, llm, nil
}

// RetryModelRouterWarmup restarts one router's failed semantic warmup.
// configstore.ErrNotFound means the plugin does not serve the router.
func (s *BifrostHTTPServer) RetryModelRouterWarmup(_ context.Context, id string) (complexity.SemanticStatusInfo, bool, error) {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return complexity.SemanticStatusInfo{}, false, fmt.Errorf("routing plugin not found: %w", err)
	}
	status, started, ok := routingPlugin.RetryModelRouterWarmup(id)
	if !ok {
		return complexity.SemanticStatusInfo{}, false, configstore.ErrNotFound
	}
	return status, started, nil
}

var _ handlers.ModelRouterClassifier = (*BifrostHTTPServer)(nil)

// ClassifyWithModelRouter classifies statements with one served router.
func (s *BifrostHTTPServer) ClassifyWithModelRouter(ctx context.Context, routerID string, inputs []routing.ModelRouterClassifyInput) (routing.ModelRouterClassifyResult, error) {
	routingPlugin, err := s.getRoutingPlugin()
	if err != nil {
		return routing.ModelRouterClassifyResult{}, fmt.Errorf("routing plugin not found: %w", err)
	}
	return routingPlugin.ClassifyWithModelRouter(ctx, routerID, inputs)
}
