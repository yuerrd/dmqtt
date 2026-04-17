package ratelimit

import "golang.org/x/time/rate"

type GlobalLimiter struct {
	ingress *rate.Limiter
	connect *rate.Limiter
}

func NewGlobalLimiter(cfg *GlobalConfig) *GlobalLimiter {
	return &GlobalLimiter{
		ingress: rate.NewLimiter(rate.Limit(cfg.IngressRate), cfg.IngressBurst),
		connect: rate.NewLimiter(rate.Limit(cfg.ConnectRate), cfg.ConnectBurst),
	}
}

func (gl *GlobalLimiter) AllowIngress() error {
	if !gl.ingress.Allow() { return ErrGlobalIngressLimit }
	return nil
}

func (gl *GlobalLimiter) AllowConnect() error {
	if !gl.connect.Allow() { return ErrGlobalConnectLimit }
	return nil
}

func (gl *GlobalLimiter) SetScaleFactor(factor float64) {
	baseIngress := float64(gl.ingress.Burst()) / 2.0
	gl.ingress.SetLimit(rate.Limit(baseIngress * factor))
}
