package rule

import (
	"context"

	"github.com/langzp/dmqtt/internal/plugin"
)

// RuleInterceptor bridges the rule engine into the plugin interceptor chain.
// It implements plugin.Interceptor and plugin.OnPublishInterceptor.
// It never rejects or modifies the original message — actions execute async.
type RuleInterceptor struct {
	engine *Engine
}

// NewRuleInterceptor creates a new RuleInterceptor wrapping the given engine.
func NewRuleInterceptor(engine *Engine) *RuleInterceptor {
	return &RuleInterceptor{engine: engine}
}

func (r *RuleInterceptor) Name() string { return "rule-engine" }
func (r *RuleInterceptor) Init() error  { return nil }
func (r *RuleInterceptor) Close() error { r.engine.Close(); return nil }

// OnPublish evaluates all rules against the published message.
// Always returns nil — never blocks or rejects the original publish.
func (r *RuleInterceptor) OnPublish(ctx context.Context, evt *plugin.PublishEvent) error {
	r.engine.Evaluate(evt.Topic, evt.Payload, evt.QoS, evt.ClientID)
	return nil
}

// Compile-time interface checks.
var (
	_ plugin.Interceptor          = (*RuleInterceptor)(nil)
	_ plugin.OnPublishInterceptor = (*RuleInterceptor)(nil)
)
