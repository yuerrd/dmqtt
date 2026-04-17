package circuitbreaker

import (
	"testing"
	"time"
)

func TestCircuitBreaker_DefaultClosed(t *testing.T) {
	cb := New(DefaultConfig())
	if cb.State() != StateClosed {
		t.Fatalf("expected Closed, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("Closed breaker should allow requests")
	}
}

func TestCircuitBreaker_OpensOnHighErrorRate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ErrorThreshold = 0.5
	cfg.MinRequests = 5
	cfg.WindowSize = time.Second
	cfg.OpenDuration = 100 * time.Millisecond
	cb := New(cfg)

	for i := 0; i < 10; i++ {
		cb.Allow()
		if i < 6 {
			cb.RecordFailure()
		} else {
			cb.RecordSuccess()
		}
	}

	if cb.State() != StateOpen {
		t.Fatalf("expected Open after high error rate, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("Open breaker should reject requests")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ErrorThreshold = 0.5
	cfg.MinRequests = 3
	cfg.WindowSize = time.Second
	cfg.OpenDuration = 50 * time.Millisecond
	cfg.HalfOpenMax = 2
	cb := New(cfg)

	for i := 0; i < 5; i++ {
		cb.Allow()
		cb.RecordFailure()
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected Open, got %v", cb.State())
	}

	time.Sleep(60 * time.Millisecond)

	if cb.State() != StateHalfOpen {
		t.Fatalf("expected HalfOpen after timeout, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("HalfOpen should allow probe requests")
	}
}

func TestCircuitBreaker_HalfOpenToClosedOnSuccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ErrorThreshold = 0.5
	cfg.MinRequests = 3
	cfg.WindowSize = time.Second
	cfg.OpenDuration = 50 * time.Millisecond
	cfg.HalfOpenMax = 2
	cb := New(cfg)

	for i := 0; i < 5; i++ {
		cb.Allow()
		cb.RecordFailure()
	}
	time.Sleep(60 * time.Millisecond)

	for i := 0; i < cfg.HalfOpenMax; i++ {
		cb.Allow()
		cb.RecordSuccess()
	}

	if cb.State() != StateClosed {
		t.Fatalf("expected Closed after successful probes, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToOpenOnFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ErrorThreshold = 0.5
	cfg.MinRequests = 3
	cfg.WindowSize = time.Second
	cfg.OpenDuration = 50 * time.Millisecond
	cfg.HalfOpenMax = 2
	cb := New(cfg)

	for i := 0; i < 5; i++ {
		cb.Allow()
		cb.RecordFailure()
	}
	time.Sleep(60 * time.Millisecond)

	cb.Allow()
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected Open after HalfOpen failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ErrorThreshold = 0.5
	cfg.MinRequests = 3
	cb := New(cfg)

	for i := 0; i < 5; i++ {
		cb.Allow()
		cb.RecordFailure()
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected Open, got %v", cb.State())
	}

	cb.Reset()
	if cb.State() != StateClosed {
		t.Fatalf("expected Closed after reset, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("should allow after reset")
	}
}
