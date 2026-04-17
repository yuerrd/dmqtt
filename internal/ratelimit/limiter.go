package ratelimit

import "errors"

// BackpressureLevel indicates the severity of system backpressure.
type BackpressureLevel int

const (
	BPLevelNone     BackpressureLevel = iota // Normal
	BPLevelMild                              // Delay new connections
	BPLevelModerate                          // Reject new subscriptions
	BPLevelSevere                            // Reject new publishes
	BPLevelCritical                          // Disconnect slow consumers
)

// Errors returned by rate limiters.
var (
	ErrGlobalIngressLimit = errors.New("global ingress rate limit exceeded")
	ErrGlobalConnectLimit = errors.New("global connect rate limit exceeded")
	ErrClientRateLimit    = errors.New("client message rate limit exceeded")
	ErrClientBlacklisted  = errors.New("client blacklisted")
	ErrTopicRateLimit     = errors.New("topic rate limit exceeded")
	ErrTopicHotspot       = errors.New("topic hotspot detected")
	ErrMessageTooLarge    = errors.New("message payload too large")
	ErrBackpressure       = errors.New("backpressure active")
	ErrTenantRateLimit    = errors.New("tenant rate limit exceeded")
)

// RateLimiter is the unified rate limiting interface used by the broker.
type RateLimiter interface {
	AllowPublish(clientID, topic string, payloadSize int) error
	AllowConnect(clientID string) error
	AllowSubscribe(clientID, topic string) error
	GetBackpressureLevel() BackpressureLevel
}

// NoopRateLimiter allows all operations. Used when rate limiting is disabled.
type NoopRateLimiter struct{}

func (n *NoopRateLimiter) AllowPublish(_, _ string, _ int) error        { return nil }
func (n *NoopRateLimiter) AllowConnect(_ string) error                   { return nil }
func (n *NoopRateLimiter) AllowSubscribe(_, _ string) error              { return nil }
func (n *NoopRateLimiter) GetBackpressureLevel() BackpressureLevel       { return BPLevelNone }
