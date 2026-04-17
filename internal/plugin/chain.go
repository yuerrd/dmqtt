package plugin

import (
	"context"
	"log/slog"
	"time"

	"github.com/langzp/dmqtt/internal/circuitbreaker"
)

// InterceptorOption configures per-interceptor behavior.
type InterceptorOption func(*interceptorConfig)

type interceptorConfig struct {
	timeout    time.Duration
	breakerCfg circuitbreaker.Config
}

func defaultInterceptorConfig() interceptorConfig {
	return interceptorConfig{
		timeout: 100 * time.Millisecond,
		breakerCfg: circuitbreaker.Config{
			ErrorThreshold: 0.5,
			WindowSize:     10 * time.Second,
			OpenDuration:   30 * time.Second,
			HalfOpenMax:    3,
			MinRequests:    5,
		},
	}
}

// WithTimeout sets the per-call timeout for an interceptor.
func WithTimeout(d time.Duration) InterceptorOption {
	return func(c *interceptorConfig) { c.timeout = d }
}

// WithBreakerThreshold sets the error rate threshold for the breaker.
func WithBreakerThreshold(rate float64) InterceptorOption {
	return func(c *interceptorConfig) { c.breakerCfg.ErrorThreshold = rate }
}

// WithBreakerResetInterval sets the open duration for the breaker.
func WithBreakerResetInterval(d time.Duration) InterceptorOption {
	return func(c *interceptorConfig) { c.breakerCfg.OpenDuration = d }
}

type registeredInterceptor struct {
	interceptor Interceptor
	executor    *Executor
}

// InterceptorChain manages registered interceptors and dispatches events.
type InterceptorChain struct {
	interceptors []registeredInterceptor
}

// NewInterceptorChain creates an empty chain.
func NewInterceptorChain() *InterceptorChain {
	return &InterceptorChain{}
}

// Register adds an interceptor to the chain with optional configuration.
func (c *InterceptorChain) Register(i Interceptor, opts ...InterceptorOption) {
	cfg := defaultInterceptorConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	c.interceptors = append(c.interceptors, registeredInterceptor{
		interceptor: i,
		executor:    NewExecutor(i.Name(), cfg.timeout, cfg.breakerCfg),
	})
}

// InitAll initializes all registered interceptors.
func (c *InterceptorChain) InitAll() error {
	for _, ri := range c.interceptors {
		if err := ri.interceptor.Init(); err != nil {
			return err
		}
	}
	return nil
}

// CloseAll closes all registered interceptors.
func (c *InterceptorChain) CloseAll() error {
	for _, ri := range c.interceptors {
		if err := ri.interceptor.Close(); err != nil {
			return err
		}
	}
	return nil
}

// OnConnect dispatches to interceptors implementing OnConnectInterceptor.
func (c *InterceptorChain) OnConnect(ctx context.Context, evt *ConnectEvent) error {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnConnectInterceptor)
		if !ok {
			continue
		}
		err := ri.executor.Run(ctx, func(ctx context.Context) error {
			return hook.OnConnect(ctx, evt)
		})
		if err == ErrInterceptorSkipped {
			slog.Warn("interceptor skipped (breaker open)", "name", ri.interceptor.Name(), "hook", "OnConnect")
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// OnPublish dispatches to interceptors implementing OnPublishInterceptor.
func (c *InterceptorChain) OnPublish(ctx context.Context, evt *PublishEvent) error {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnPublishInterceptor)
		if !ok {
			continue
		}
		err := ri.executor.Run(ctx, func(ctx context.Context) error {
			return hook.OnPublish(ctx, evt)
		})
		if err == ErrInterceptorSkipped {
			slog.Warn("interceptor skipped (breaker open)", "name", ri.interceptor.Name(), "hook", "OnPublish")
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// OnSubscribe dispatches to interceptors implementing OnSubscribeInterceptor.
func (c *InterceptorChain) OnSubscribe(ctx context.Context, evt *SubscribeEvent) error {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnSubscribeInterceptor)
		if !ok {
			continue
		}
		err := ri.executor.Run(ctx, func(ctx context.Context) error {
			return hook.OnSubscribe(ctx, evt)
		})
		if err == ErrInterceptorSkipped {
			slog.Warn("interceptor skipped (breaker open)", "name", ri.interceptor.Name(), "hook", "OnSubscribe")
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// OnDelivery dispatches to interceptors implementing OnDeliveryInterceptor.
func (c *InterceptorChain) OnDelivery(ctx context.Context, evt *DeliveryEvent) error {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnDeliveryInterceptor)
		if !ok {
			continue
		}
		err := ri.executor.Run(ctx, func(ctx context.Context) error {
			return hook.OnDelivery(ctx, evt)
		})
		if err == ErrInterceptorSkipped {
			slog.Warn("interceptor skipped (breaker open)", "name", ri.interceptor.Name(), "hook", "OnDelivery")
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// OnDisconnect dispatches asynchronously to interceptors implementing OnDisconnectInterceptor.
func (c *InterceptorChain) OnDisconnect(evt *DisconnectEvent) {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnDisconnectInterceptor)
		if !ok {
			continue
		}
		go func(h OnDisconnectInterceptor, name string) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("interceptor panic", "name", name, "hook", "OnDisconnect", "recover", r)
				}
			}()
			h.OnDisconnect(evt)
		}(hook, ri.interceptor.Name())
	}
}

// OnSessionExpired dispatches asynchronously to interceptors implementing OnSessionExpiredInterceptor.
func (c *InterceptorChain) OnSessionExpired(evt *SessionExpiredEvent) {
	for _, ri := range c.interceptors {
		hook, ok := ri.interceptor.(OnSessionExpiredInterceptor)
		if !ok {
			continue
		}
		go func(h OnSessionExpiredInterceptor, name string) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("interceptor panic", "name", name, "hook", "OnSessionExpired", "recover", r)
				}
			}()
			h.OnSessionExpired(evt)
		}(hook, ri.interceptor.Name())
	}
}
