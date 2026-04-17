package rule

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/metrics"
)

// PublishFunc is the callback to republish a message into the broker.
type PublishFunc func(topic string, payload []byte, qos byte)

// ActionExecutor dispatches rule actions with bounded concurrency.
type ActionExecutor struct {
	publishFn  PublishFunc
	sem        chan struct{} // semaphore for worker pool
	httpClient *http.Client
	wg         sync.WaitGroup
}

// ActionExecutorConfig configures the action executor.
type ActionExecutorConfig struct {
	WorkerPoolSize int
	WebhookTimeout time.Duration
}

// NewActionExecutor creates an ActionExecutor.
func NewActionExecutor(publishFn PublishFunc, cfg ActionExecutorConfig) *ActionExecutor {
	if cfg.WorkerPoolSize <= 0 {
		cfg.WorkerPoolSize = 64
	}
	if cfg.WebhookTimeout <= 0 {
		cfg.WebhookTimeout = 5 * time.Second
	}
	return &ActionExecutor{
		publishFn: publishFn,
		sem:       make(chan struct{}, cfg.WorkerPoolSize),
		httpClient: &http.Client{
			Timeout: cfg.WebhookTimeout,
		},
	}
}

// webhookPayload is the JSON body sent to webhook URLs.
type webhookPayload struct {
	RuleID    string          `json:"rule_id"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	ClientID  string          `json:"client_id"`
	Timestamp string          `json:"timestamp"`
}

// Execute dispatches all actions for a matched rule asynchronously.
// Each action acquires a worker pool slot. If the pool is full, the action is dropped.
func (ae *ActionExecutor) Execute(ruleID string, topic string, payload []byte, qos byte, clientID string, actions []Action) {
	for _, a := range actions {
		action := a
		select {
		case ae.sem <- struct{}{}:
			metrics.RuleAction()
			ae.wg.Add(1)
			go func() {
				defer func() {
					<-ae.sem
					ae.wg.Done()
				}()
				ae.executeOne(ruleID, topic, payload, qos, clientID, action)
			}()
		default:
			metrics.RuleActionDropped()
			slog.Warn("rule action dropped: worker pool full",
				"rule_id", ruleID, "action_type", action.Type)
		}
	}
}

func (ae *ActionExecutor) executeOne(ruleID, topic string, payload []byte, qos byte, clientID string, action Action) {
	switch action.Type {
	case "publish":
		ae.executePublish(action, payload, qos)
	case "webhook":
		ae.executeWebhook(ruleID, topic, payload, clientID, action)
	default:
		slog.Error("unknown action type", "rule_id", ruleID, "type", action.Type)
	}
}

func (ae *ActionExecutor) executePublish(action Action, payload []byte, qos byte) {
	if ae.publishFn != nil {
		ae.publishFn(action.TargetTopic, payload, qos)
	}
}

func (ae *ActionExecutor) executeWebhook(ruleID, topic string, payload []byte, clientID string, action Action) {
	body := webhookPayload{
		RuleID:    ruleID,
		Topic:     topic,
		Payload:   json.RawMessage(payload),
		ClientID:  clientID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(body)
	if err != nil {
		slog.Error("marshal webhook payload", "rule_id", ruleID, "error", err)
		return
	}

	resp, err := ae.httpClient.Post(action.WebhookURL, "application/json", bytes.NewReader(data))
	if err != nil {
		metrics.RuleActionError()
		slog.Error("webhook request failed", "rule_id", ruleID, "url", action.WebhookURL, "error", err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		metrics.RuleActionError()
		slog.Error("webhook returned error", "rule_id", ruleID, "url", action.WebhookURL, "status", resp.StatusCode)
	}
}

// Close waits for all in-flight actions to complete.
func (ae *ActionExecutor) Close() {
	ae.wg.Wait()
}
