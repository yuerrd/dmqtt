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
