package rule

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Rule represents a single rule loaded from YAML config.
type Rule struct {
	ID      string   `yaml:"rule_id"`
	Enabled bool     `yaml:"enabled"`
	Source  Source   `yaml:"source"`
	Filter  string   `yaml:"filter"`
	Actions []Action `yaml:"actions"`
}

// Source defines the topic filter for rule matching.
type Source struct {
	Topic string `yaml:"topic"` // MQTT topic filter with +/# wildcards
}

// Action defines what happens when a rule matches.
type Action struct {
	Type        string `yaml:"type"`         // "publish" or "webhook"
	TargetTopic string `yaml:"target_topic"` // for publish
	WebhookURL  string `yaml:"webhook"`      // for webhook
}

// rulesFile is the top-level YAML structure.
type rulesFile struct {
	Rules []Rule `yaml:"rules"`
}

// LoadRulesFromFile reads and parses rules from a YAML file.
func LoadRulesFromFile(path string) ([]Rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rules file: %w", err)
	}
	return ParseRules(data)
}

// ParseRules parses rules from YAML bytes.
func ParseRules(data []byte) ([]Rule, error) {
	var f rulesFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse rules YAML: %w", err)
	}
	for i, r := range f.Rules {
		if r.ID == "" {
			return nil, fmt.Errorf("rule at index %d: missing rule_id", i)
		}
		if r.Source.Topic == "" {
			return nil, fmt.Errorf("rule %q: missing source.topic", r.ID)
		}
		for j, a := range r.Actions {
			switch a.Type {
			case "publish":
				if a.TargetTopic == "" {
					return nil, fmt.Errorf("rule %q action %d: publish requires target_topic", r.ID, j)
				}
			case "webhook":
				if a.WebhookURL == "" {
					return nil, fmt.Errorf("rule %q action %d: webhook requires webhook URL", r.ID, j)
				}
			default:
				return nil, fmt.Errorf("rule %q action %d: unknown type %q", r.ID, j, a.Type)
			}
		}
	}
	return f.Rules, nil
}
