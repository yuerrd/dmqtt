package plugin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yuerrd/dmqtt/internal/circuitbreaker"
)

// ErrInterceptorSkipped is returned when the circuit breaker is open.
var ErrInterceptorSkipped = errors.New("interceptor skipped: circuit breaker open")

// Executor wraps an interceptor call with timeout and circuit breaker protection.
type Executor struct {
	name    string
	timeout time.Duration
	breaker *circuitbreaker.CircuitBreaker
}

// NewExecutor creates an Executor with the given timeout and breaker config.
func NewExecutor(name string, timeout time.Duration, breakerCfg circuitbreaker.Config) *Executor {
	return &Executor{
		name:    name,
		timeout: timeout,
		breaker: circuitbreaker.New(breakerCfg),
	}
}

// Run executes fn with timeout and circuit breaker protection.
// Returns ErrInterceptorSkipped if the breaker is open.
func (e *Executor) Run(parent context.Context, fn func(ctx context.Context) error) error {
	if err := e.breaker.AllowOrError(); err != nil {
		return ErrInterceptorSkipped
	}

	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("interceptor panic: %v", r)
			}
		}()
		done <- fn(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			e.breaker.RecordFailure()
		} else {
			e.breaker.RecordSuccess()
		}
		return err
	case <-ctx.Done():
		e.breaker.RecordFailure()
		return ctx.Err()
	}
}
