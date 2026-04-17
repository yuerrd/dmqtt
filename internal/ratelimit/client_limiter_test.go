package ratelimit

import (
	"testing"
	"time"
)

func TestClientLimiter_AllowsWithinRate(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 100, MsgBurst: 200, BlacklistTTL: time.Minute, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	for i := 0; i < 100; i++ {
		if err := cl.AllowMessage("client1"); err != nil {
			t.Fatalf("AllowMessage should succeed at i=%d: %v", i, err)
		}
	}
}

func TestClientLimiter_RejectsOverBurst(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 1, MsgBurst: 3, BlacklistTTL: time.Minute, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	rejected := 0
	for i := 0; i < 20; i++ {
		if err := cl.AllowMessage("client1"); err != nil { rejected++ }
	}
	if rejected == 0 { t.Fatal("expected some rejections") }
}

func TestClientLimiter_IndependentPerClient(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 1, MsgBurst: 3, BlacklistTTL: time.Minute, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	for i := 0; i < 10; i++ { cl.AllowMessage("client1") }
	if err := cl.AllowMessage("client2"); err != nil {
		t.Fatalf("client2 should be independent: %v", err)
	}
}

func TestClientLimiter_Blacklist(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 1, MsgBurst: 3, BlacklistTTL: 100 * time.Millisecond, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	cl.Blacklist("client1")
	if err := cl.AllowMessage("client1"); err != ErrClientBlacklisted {
		t.Fatalf("expected ErrClientBlacklisted, got %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := cl.AllowMessage("client1"); err == ErrClientBlacklisted {
		t.Fatal("blacklist should have expired")
	}
}

func TestClientLimiter_IsBlacklisted(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 100, MsgBurst: 200, BlacklistTTL: time.Minute, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	if cl.IsBlacklisted("client1") { t.Fatal("should not be blacklisted initially") }
	cl.Blacklist("client1")
	if !cl.IsBlacklisted("client1") { t.Fatal("should be blacklisted") }
}

func TestClientLimiter_RemoveClient(t *testing.T) {
	cfg := &ClientConfig{MsgRate: 1, MsgBurst: 3, BlacklistTTL: time.Minute, CleanupInterval: time.Hour}
	cl := NewClientLimiter(cfg)
	for i := 0; i < 10; i++ { cl.AllowMessage("client1") }
	cl.RemoveClient("client1")
	if err := cl.AllowMessage("client1"); err != nil {
		t.Fatalf("should succeed with fresh limiter: %v", err)
	}
}
