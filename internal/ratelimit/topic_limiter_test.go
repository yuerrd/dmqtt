package ratelimit

import (
	"testing"
	"time"
)

func TestTopicLimiter_NoLimitByDefault(t *testing.T) {
	cfg := &TopicConfig{DefaultRate: 0, HotspotThreshold: 100_000, HotspotWindow: 10 * time.Second, TopicConfigs: make(map[string]TopicLimitEntry)}
	tl := NewTopicLimiter(cfg)
	if err := tl.AllowPublish("sensors/temp"); err != nil {
		t.Fatalf("should allow when default rate is 0: %v", err)
	}
}

func TestTopicLimiter_ConfiguredTopicLimit(t *testing.T) {
	cfg := &TopicConfig{
		DefaultRate: 0, HotspotThreshold: 100_000, HotspotWindow: 10 * time.Second,
		TopicConfigs: map[string]TopicLimitEntry{"alerts/fire": {MaxPublishRate: 1, MaxBurst: 3}},
	}
	tl := NewTopicLimiter(cfg)
	rejected := 0
	for i := 0; i < 20; i++ {
		if err := tl.AllowPublish("alerts/fire"); err != nil { rejected++ }
	}
	if rejected == 0 { t.Fatal("expected rejections for configured topic") }
	if err := tl.AllowPublish("sensors/temp"); err != nil {
		t.Fatalf("unconfigured topic should pass: %v", err)
	}
}

func TestTopicLimiter_DefaultRate(t *testing.T) {
	cfg := &TopicConfig{DefaultRate: 1, DefaultBurst: 3, HotspotThreshold: 100_000, HotspotWindow: 10 * time.Second, TopicConfigs: make(map[string]TopicLimitEntry)}
	tl := NewTopicLimiter(cfg)
	rejected := 0
	for i := 0; i < 20; i++ {
		if err := tl.AllowPublish("any/topic"); err != nil { rejected++ }
	}
	if rejected == 0 { t.Fatal("expected rejections with default rate") }
}

func TestTopicLimiter_HotspotDetection(t *testing.T) {
	cfg := &TopicConfig{DefaultRate: 0, HotspotThreshold: 10, HotspotWindow: time.Second, TopicConfigs: make(map[string]TopicLimitEntry)}
	tl := NewTopicLimiter(cfg)
	for i := 0; i < 50; i++ { tl.RecordPublish("hot/topic") }
	if !tl.IsHotspot("hot/topic") { t.Fatal("should detect hot/topic as hotspot") }
	if tl.IsHotspot("cold/topic") { t.Fatal("cold/topic should not be a hotspot") }
}
