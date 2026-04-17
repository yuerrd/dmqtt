package rule

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/langzp/dmqtt/internal/metrics"
)

// EngineConfig configures the rule engine.
type EngineConfig struct {
	WorkerPoolSize int
	WebhookTimeout time.Duration
}

// Engine manages rules and evaluates messages against them.
type Engine struct {
	mu       sync.RWMutex
	rules    []*compiledRule
	env      *cel.Env
	executor *ActionExecutor
}

// NewEngine creates a new rule engine.
func NewEngine(publishFn PublishFunc, cfg EngineConfig) (*Engine, error) {
	env, err := newCELEnv()
	if err != nil {
		return nil, fmt.Errorf("create CEL env: %w", err)
	}
	return &Engine{
		env: env,
		executor: NewActionExecutor(publishFn, ActionExecutorConfig{
			WorkerPoolSize: cfg.WorkerPoolSize,
			WebhookTimeout: cfg.WebhookTimeout,
		}),
	}, nil
}

// LoadRules loads and compiles rules from a YAML file.
// On success, atomically swaps the active rules.
// On failure, keeps existing rules and returns error.
func (e *Engine) LoadRules(path string) error {
	rawRules, err := LoadRulesFromFile(path)
	if err != nil {
		return err
	}
	return e.setRules(rawRules)
}

// LoadRulesFromBytes loads and compiles rules from YAML bytes.
// Useful for testing.
func (e *Engine) LoadRulesFromBytes(data []byte) error {
	rawRules, err := ParseRules(data)
	if err != nil {
		return err
	}
	return e.setRules(rawRules)
}

func (e *Engine) setRules(rawRules []Rule) error {
	var compiled []*compiledRule
	for _, r := range rawRules {
		if !r.Enabled {
			slog.Info("rule disabled, skipping", "rule_id", r.ID)
			continue
		}
		cr, err := compileRule(e.env, r)
		if err != nil {
			return fmt.Errorf("rule %q: %w", r.ID, err)
		}
		compiled = append(compiled, cr)
		slog.Info("rule compiled", "rule_id", r.ID, "topic", r.Source.Topic)
	}

	e.mu.Lock()
	e.rules = compiled
	e.mu.Unlock()

	slog.Info("rules loaded", "count", len(compiled))
	return nil
}

// Evaluate matches the message topic against all rules, evaluates CEL filters,
// and dispatches actions for matching rules. Actions run asynchronously.
func (e *Engine) Evaluate(topic string, payload []byte, qos byte, clientID string) {
	e.mu.RLock()
	rules := e.rules
	e.mu.RUnlock()

	for _, cr := range rules {
		if !topicMatch(cr.Source.Topic, topic) {
			continue
		}

		metrics.RuleEvaluation()
		match, err := cr.evaluate(topic, payload, qos, clientID)
		if err != nil {
			slog.Error("rule evaluation error", "rule_id", cr.ID, "error", err)
			continue
		}
		if !match {
			continue
		}

		metrics.RuleMatch()
		slog.Debug("rule matched", "rule_id", cr.ID, "topic", topic)
		e.executor.Execute(cr.ID, topic, payload, qos, clientID, cr.Actions)
	}
}

// RuleCount returns the number of active compiled rules.
func (e *Engine) RuleCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.rules)
}

// Close shuts down the engine and waits for pending actions.
func (e *Engine) Close() {
	e.executor.Close()
}

// topicMatch checks if a topic name matches a subscription filter.
// Duplicated from broker package to avoid import cycle.
func topicMatch(filter, topic string) bool {
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) > 0 && (filter[0] == '+' || filter[0] == '#') {
			return false
		}
	}
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")
	for i := 0; i < len(filterParts); i++ {
		if filterParts[i] == "#" {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if filterParts[i] == "+" {
			continue
		}
		if filterParts[i] != topicParts[i] {
			return false
		}
	}
	return len(filterParts) == len(topicParts)
}
