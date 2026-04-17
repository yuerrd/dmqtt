package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type Config struct {
	ErrorThreshold float64
	WindowSize     time.Duration
	OpenDuration   time.Duration
	HalfOpenMax    int
	MinRequests    int64
}

func DefaultConfig() Config {
	return Config{
		ErrorThreshold: 0.1,
		WindowSize:     5 * time.Second,
		OpenDuration:   30 * time.Second,
		HalfOpenMax:    5,
		MinRequests:    10,
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting requests.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// AllowOrError returns nil if the request is allowed, or ErrCircuitOpen if the breaker is open.
func (cb *CircuitBreaker) AllowOrError() error {
	if cb.Allow() {
		return nil
	}
	return ErrCircuitOpen
}

type CircuitBreaker struct {
	mu           sync.Mutex
	state        State
	failCount    int64
	successCount int64
	totalCount   int64
	halfOpenSucc int
	openedAt     time.Time
	config       Config
}

func New(cfg Config) *CircuitBreaker {
	return &CircuitBreaker{state: StateClosed, config: cfg}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.openedAt) > cb.config.OpenDuration {
			cb.state = StateHalfOpen
			cb.halfOpenSucc = 0
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.successCount++
	cb.totalCount++
	if cb.state == StateHalfOpen {
		cb.halfOpenSucc++
		if cb.halfOpenSucc >= cb.config.HalfOpenMax {
			cb.reset()
		}
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failCount++
	cb.totalCount++
	switch cb.state {
	case StateClosed:
		if cb.totalCount >= cb.config.MinRequests {
			errorRate := float64(cb.failCount) / float64(cb.totalCount)
			if errorRate >= cb.config.ErrorThreshold {
				cb.trip()
			}
		}
	case StateHalfOpen:
		cb.trip()
	}
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateOpen && time.Since(cb.openedAt) > cb.config.OpenDuration {
		cb.state = StateHalfOpen
		cb.halfOpenSucc = 0
	}
	return cb.state
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.reset()
}

func (cb *CircuitBreaker) trip() {
	cb.state = StateOpen
	cb.openedAt = time.Now()
	cb.failCount = 0
	cb.successCount = 0
	cb.totalCount = 0
	cb.halfOpenSucc = 0
}

func (cb *CircuitBreaker) reset() {
	cb.state = StateClosed
	cb.failCount = 0
	cb.successCount = 0
	cb.totalCount = 0
	cb.halfOpenSucc = 0
}
