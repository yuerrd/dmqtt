package rule

import (
	"testing"
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
