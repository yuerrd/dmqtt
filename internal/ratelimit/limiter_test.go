package ratelimit

import "testing"

func TestNoopRateLimiter_AllowsEverything(t *testing.T) {
	rl := &NoopRateLimiter{}

	if err := rl.AllowPublish("client1", "topic/a", 100); err != nil {
		t.Fatalf("AllowPublish should succeed: %v", err)
	}
	if err := rl.AllowConnect("client1"); err != nil {
		t.Fatalf("AllowConnect should succeed: %v", err)
	}
	if err := rl.AllowSubscribe("client1", "topic/a"); err != nil {
		t.Fatalf("AllowSubscribe should succeed: %v", err)
	}
	if level := rl.GetBackpressureLevel(); level != BPLevelNone {
		t.Fatalf("backpressure level should be None, got %d", level)
	}
}

func TestAggregateRateLimiter_AllowsWhenAllPass(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Global.IngressRate = 10000
	cfg.Global.IngressBurst = 20000
	cfg.Client.MsgRate = 100
	cfg.Client.MsgBurst = 200
	cfg.MaxMessageSize = 1024

	rl := NewAggregateRateLimiter(cfg)

	if err := rl.AllowPublish("client1", "topic/a", 100); err != nil {
		t.Fatalf("should allow: %v", err)
	}
	if err := rl.AllowConnect("client1"); err != nil {
		t.Fatalf("should allow connect: %v", err)
	}
	if err := rl.AllowSubscribe("client1", "topic/a"); err != nil {
		t.Fatalf("should allow subscribe: %v", err)
	}
}

func TestAggregateRateLimiter_RejectsOversizedMessage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MaxMessageSize = 100

	rl := NewAggregateRateLimiter(cfg)

	if err := rl.AllowPublish("client1", "topic/a", 200); err != ErrMessageTooLarge {
		t.Fatalf("expected ErrMessageTooLarge, got %v", err)
	}
}

func TestAggregateRateLimiter_ClientRateLimitIntegration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Client.MsgRate = 1
	cfg.Client.MsgBurst = 3

	rl := NewAggregateRateLimiter(cfg)

	rejected := 0
	for i := 0; i < 20; i++ {
		if err := rl.AllowPublish("client1", "topic/a", 10); err != nil {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("expected some client rate limit rejections")
	}
}

func TestAggregateRateLimiter_BackpressureLevelForwarded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.QueueSizeMax = 100
	cfg.Backpressure.SevereThreshold = 0.7

	rl := NewAggregateRateLimiter(cfg)
	rl.UpdateBackpressure(80)

	if level := rl.GetBackpressureLevel(); level != BPLevelSevere {
		t.Fatalf("expected BPLevelSevere, got %d", level)
	}
}

func TestAggregateRateLimiter_DisabledAllowsEverything(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	rl := NewAggregateRateLimiter(cfg)

	for i := 0; i < 1000; i++ {
		if err := rl.AllowPublish("c", "t", 999999); err != nil {
			t.Fatalf("disabled should allow everything: %v", err)
		}
	}
}
