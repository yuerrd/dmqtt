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

func (n *NoopRateLimiter) AllowPublish(_, _ string, _ int) error   { return nil }
func (n *NoopRateLimiter) AllowConnect(_ string) error             { return nil }
func (n *NoopRateLimiter) AllowSubscribe(_, _ string) error        { return nil }
func (n *NoopRateLimiter) GetBackpressureLevel() BackpressureLevel { return BPLevelNone }

// AggregateRateLimiter chains all rate limiting layers.
type AggregateRateLimiter struct {
	global   *GlobalLimiter
	tenant   TenantLimiter
	client   *ClientLimiter
	topic    *TopicLimiter
	bp       *BackpressureController
	adaptive *AdaptiveController
	detector *MaliciousDetector
	config   *Config
}

// NewAggregateRateLimiter creates a fully configured rate limiter from config.
func NewAggregateRateLimiter(cfg *Config) *AggregateRateLimiter {
	return &AggregateRateLimiter{
		global:   NewGlobalLimiter(&cfg.Global),
		tenant:   &NoopTenantLimiter{},
		client:   NewClientLimiter(&cfg.Client),
		topic:    NewTopicLimiter(&cfg.Topic),
		bp:       NewBackpressureController(&cfg.Backpressure),
		adaptive: NewAdaptiveController(&cfg.Adaptive),
		detector: NewMaliciousDetector(&cfg.Detector),
		config:   cfg,
	}
}

// AllowPublish checks all rate limiting layers for a publish operation.
func (a *AggregateRateLimiter) AllowPublish(clientID, topic string, payloadSize int) error {
	if !a.config.Enabled {
		return nil
	}

	// 1. Backpressure
	if level := a.bp.Level(); level >= BPLevelSevere {
		return ErrBackpressure
	}

	// 2. Malicious client check
	if a.detector.IsMalicious(clientID) {
		return ErrClientBlacklisted
	}

	// 3. Message size
	if payloadSize > a.config.MaxMessageSize {
		return ErrMessageTooLarge
	}

	// 4. Global ingress
	if err := a.global.AllowIngress(); err != nil {
		return err
	}

	// 5. Client rate
	if err := a.client.AllowMessage(clientID); err != nil {
		return err
	}

	// 6. Topic rate
	if err := a.topic.AllowPublish(topic); err != nil {
		return err
	}

	// Record for hotspot detection
	a.topic.RecordPublish(topic)
	a.detector.RecordMessage(clientID)

	return nil
}

// AllowConnect checks global and client connect rate limits.
func (a *AggregateRateLimiter) AllowConnect(clientID string) error {
	if !a.config.Enabled {
		return nil
	}

	if level := a.bp.Level(); level >= BPLevelMild {
		return ErrBackpressure
	}

	if err := a.global.AllowConnect(); err != nil {
		return err
	}

	return nil
}

// AllowSubscribe checks rate limits for a subscribe operation.
func (a *AggregateRateLimiter) AllowSubscribe(clientID, topic string) error {
	if !a.config.Enabled {
		return nil
	}

	if level := a.bp.Level(); level >= BPLevelModerate {
		return ErrBackpressure
	}

	a.detector.RecordSubscribe(clientID, topic)

	return nil
}

// GetBackpressureLevel returns the current backpressure level.
func (a *AggregateRateLimiter) GetBackpressureLevel() BackpressureLevel {
	return a.bp.Level()
}

// UpdateBackpressure updates the backpressure level based on current queue size.
func (a *AggregateRateLimiter) UpdateBackpressure(queueSize int) {
	a.bp.Update(queueSize)
}

// RecordReconnect records a client reconnection event for malicious detection.
func (a *AggregateRateLimiter) RecordReconnect(clientID string) {
	if a.config.Enabled {
		a.detector.RecordReconnect(clientID)
	}
}

// EvaluateClient triggers malicious behavior evaluation for a client.
func (a *AggregateRateLimiter) EvaluateClient(clientID string) {
	if a.config.Enabled {
		a.detector.Evaluate(clientID)
	}
}

// BlacklistClient adds a client to the temporary blacklist.
func (a *AggregateRateLimiter) BlacklistClient(clientID string) {
	if a.config.Enabled {
		a.client.Blacklist(clientID)
	}
}
