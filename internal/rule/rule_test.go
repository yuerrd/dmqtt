package rule

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseRules_Valid(t *testing.T) {
	yaml := []byte(`
rules:
  - rule_id: rule-001
    enabled: true
    source:
      topic: "devices/+/telemetry"
    filter: "payload.temperature > 100"
    actions:
      - type: publish
        target_topic: "alerts/temperature"
      - type: webhook
        webhook: "https://example.com/hook"
`)
	rules, err := ParseRules(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	r := rules[0]
	if r.ID != "rule-001" {
		t.Errorf("expected ID rule-001, got %s", r.ID)
	}
	if !r.Enabled {
		t.Error("expected enabled=true")
	}
	if r.Source.Topic != "devices/+/telemetry" {
		t.Errorf("unexpected source topic: %s", r.Source.Topic)
	}
	if r.Filter != "payload.temperature > 100" {
		t.Errorf("unexpected filter: %s", r.Filter)
	}
	if len(r.Actions) != 2 {
		t.Fatalf("expected 2 actions, got %d", len(r.Actions))
	}
	if r.Actions[0].Type != "publish" || r.Actions[0].TargetTopic != "alerts/temperature" {
		t.Errorf("unexpected publish action: %+v", r.Actions[0])
	}
	if r.Actions[1].Type != "webhook" || r.Actions[1].WebhookURL != "https://example.com/hook" {
		t.Errorf("unexpected webhook action: %+v", r.Actions[1])
	}
}

func TestParseRules_MissingID(t *testing.T) {
	yaml := []byte(`
rules:
  - enabled: true
    source:
      topic: "test/+"
    filter: "true"
    actions:
      - type: publish
        target_topic: "out"
`)
	_, err := ParseRules(yaml)
	if err == nil {
		t.Fatal("expected error for missing rule_id")
	}
}

func TestParseRules_InvalidActionType(t *testing.T) {
	yaml := []byte(`
rules:
  - rule_id: bad
    enabled: true
    source:
      topic: "test/+"
    filter: "true"
    actions:
      - type: email
`)
	_, err := ParseRules(yaml)
	if err == nil {
		t.Fatal("expected error for unknown action type")
	}
}

func TestParseRules_PublishMissingTarget(t *testing.T) {
	yaml := []byte(`
rules:
  - rule_id: bad
    enabled: true
    source:
      topic: "test/+"
    filter: "true"
    actions:
      - type: publish
`)
	_, err := ParseRules(yaml)
	if err == nil {
		t.Fatal("expected error for publish without target_topic")
	}
}

func TestCompileRule_ValidExpression(t *testing.T) {
	env, err := newCELEnv()
	if err != nil {
		t.Fatalf("failed to create CEL env: %v", err)
	}
	r := Rule{
		ID:      "test-1",
		Enabled: true,
		Filter:  "payload.temperature > 100",
	}
	cr, err := compileRule(env, r)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	// Should match
	match, err := cr.evaluate("test/topic", []byte(`{"temperature": 150}`), 0, "client-1")
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if !match {
		t.Error("expected match for temperature=150")
	}

	// Should not match
	match, err = cr.evaluate("test/topic", []byte(`{"temperature": 50}`), 0, "client-1")
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if match {
		t.Error("expected no match for temperature=50")
	}
}

func TestCompileRule_InvalidExpression(t *testing.T) {
	env, err := newCELEnv()
	if err != nil {
		t.Fatalf("failed to create CEL env: %v", err)
	}
	r := Rule{
		ID:     "bad",
		Filter: "payload.foo &&& invalid",
	}
	_, err = compileRule(env, r)
	if err == nil {
		t.Fatal("expected compilation error")
	}
}

func TestCompiledRule_NonJSONPayload(t *testing.T) {
	env, err := newCELEnv()
	if err != nil {
		t.Fatalf("failed to create CEL env: %v", err)
	}
	r := Rule{
		ID:     "test-2",
		Filter: "payload.x > 0",
	}
	cr, err := compileRule(env, r)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}
	match, err := cr.evaluate("test", []byte("not json"), 0, "c1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match {
		t.Error("non-JSON payload should not match")
	}
}

func TestCompileRule_EmptyFilter(t *testing.T) {
	env, err := newCELEnv()
	if err != nil {
		t.Fatalf("failed to create CEL env: %v", err)
	}
	r := Rule{
		ID:     "always",
		Filter: "",
	}
	cr, err := compileRule(env, r)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}
	match, err := cr.evaluate("test", []byte(`{}`), 0, "c1")
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if !match {
		t.Error("empty filter should match (defaults to true)")
	}
}

func TestCompileRule_TopicAndQoSAccess(t *testing.T) {
	env, err := newCELEnv()
	if err != nil {
		t.Fatalf("failed to create CEL env: %v", err)
	}
	r := Rule{
		ID:     "meta",
		Filter: `topic == "devices/123/data" && qos >= 1`,
	}
	cr, err := compileRule(env, r)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}
	match, err := cr.evaluate("devices/123/data", []byte(`{}`), 1, "c1")
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if !match {
		t.Error("expected match for topic=devices/123/data, qos=1")
	}

	match, err = cr.evaluate("other/topic", []byte(`{}`), 0, "c1")
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if match {
		t.Error("expected no match for wrong topic")
	}
}

func TestActionExecutor_Publish(t *testing.T) {
	var published atomic.Int32
	var capturedTopic string
	publishFn := func(topic string, payload []byte, qos byte) {
		capturedTopic = topic
		published.Add(1)
	}
	ae := NewActionExecutor(publishFn, ActionExecutorConfig{
		WorkerPoolSize: 4,
		WebhookTimeout: 1 * time.Second,
	})

	actions := []Action{{Type: "publish", TargetTopic: "alerts/temp"}}
	ae.Execute("r1", "devices/1/data", []byte(`{"t":100}`), 0, "c1", actions)

	ae.Close()
	if published.Load() != 1 {
		t.Fatalf("expected 1 publish, got %d", published.Load())
	}
	if capturedTopic != "alerts/temp" {
		t.Errorf("expected topic alerts/temp, got %s", capturedTopic)
	}
}

func TestActionExecutor_Webhook(t *testing.T) {
	var received atomic.Int32
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ae := NewActionExecutor(nil, ActionExecutorConfig{
		WorkerPoolSize: 4,
		WebhookTimeout: 2 * time.Second,
	})

	actions := []Action{{Type: "webhook", WebhookURL: srv.URL}}
	ae.Execute("r2", "test/topic", []byte(`{"val":42}`), 1, "client-1", actions)

	ae.Close()
	if received.Load() != 1 {
		t.Fatalf("expected 1 webhook call, got %d", received.Load())
	}

	var wp webhookPayload
	if err := json.Unmarshal(receivedBody, &wp); err != nil {
		t.Fatalf("unmarshal webhook body: %v", err)
	}
	if wp.RuleID != "r2" {
		t.Errorf("expected rule_id r2, got %s", wp.RuleID)
	}
	if wp.Topic != "test/topic" {
		t.Errorf("expected topic test/topic, got %s", wp.Topic)
	}
	if wp.ClientID != "client-1" {
		t.Errorf("expected client_id client-1, got %s", wp.ClientID)
	}
}

func TestActionExecutor_PoolFull(t *testing.T) {
	blocker := make(chan struct{})
	var executed atomic.Int32
	publishFn := func(topic string, payload []byte, qos byte) {
		executed.Add(1)
		<-blocker
	}
	ae := NewActionExecutor(publishFn, ActionExecutorConfig{
		WorkerPoolSize: 1,
		WebhookTimeout: 1 * time.Second,
	})

	ae.Execute("r1", "t", []byte(`{}`), 0, "c", []Action{{Type: "publish", TargetTopic: "out"}})
	time.Sleep(50 * time.Millisecond)

	ae.Execute("r2", "t", []byte(`{}`), 0, "c", []Action{{Type: "publish", TargetTopic: "out2"}})

	close(blocker)
	ae.Close()

	if executed.Load() != 1 {
		t.Errorf("expected 1 executed (second dropped), got %d", executed.Load())
	}
}
