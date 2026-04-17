package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/langzp/dmqtt/internal/circuitbreaker"
)

type slowInterceptor struct {
	delay time.Duration
}

func (s *slowInterceptor) Name() string { return "slow" }
func (s *slowInterceptor) Init() error  { return nil }
func (s *slowInterceptor) Close() error { return nil }
func (s *slowInterceptor) OnPublish(ctx context.Context, evt *PublishEvent) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type failInterceptor struct{}

func (f *failInterceptor) Name() string { return "fail" }
func (f *failInterceptor) Init() error  { return nil }
func (f *failInterceptor) Close() error { return nil }
func (f *failInterceptor) OnPublish(ctx context.Context, evt *PublishEvent) error {
	return errors.New("rejected")
}

func TestExecutor_NormalExecution(t *testing.T) {
	exec := NewExecutor("test", 100*time.Millisecond, circuitbreaker.Config{
		ErrorThreshold: 0.5,
		WindowSize:     5 * time.Second,
		OpenDuration:   1 * time.Second,
		HalfOpenMax:    2,
		MinRequests:    3,
	})

	err := exec.Run(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestExecutor_Timeout(t *testing.T) {
	exec := NewExecutor("test", 50*time.Millisecond, circuitbreaker.Config{
		ErrorThreshold: 0.5,
		WindowSize:     5 * time.Second,
		OpenDuration:   1 * time.Second,
		HalfOpenMax:    2,
		MinRequests:    3,
	})

	err := exec.Run(context.Background(), func(ctx context.Context) error {
		<-time.After(200 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestExecutor_CircuitBreakerTrips(t *testing.T) {
	exec := NewExecutor("test", 100*time.Millisecond, circuitbreaker.Config{
		ErrorThreshold: 0.5,
		WindowSize:     5 * time.Second,
		OpenDuration:   1 * time.Second,
		HalfOpenMax:    2,
		MinRequests:    3,
	})

	// Trigger enough failures to trip the breaker
	for i := 0; i < 5; i++ {
		exec.Run(context.Background(), func(ctx context.Context) error {
			return errors.New("fail")
		})
	}

	// Next call should be skipped (circuit open)
	err := exec.Run(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrInterceptorSkipped) {
		t.Fatalf("expected ErrInterceptorSkipped, got %v", err)
	}
}
