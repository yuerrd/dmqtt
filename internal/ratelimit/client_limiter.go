package ratelimit

import (
	"sync"
	"time"
	"golang.org/x/time/rate"
)

type ClientLimiter struct {
	limiters  sync.Map
	blacklist sync.Map
	config    *ClientConfig
}

func NewClientLimiter(cfg *ClientConfig) *ClientLimiter {
	return &ClientLimiter{config: cfg}
}

func (cl *ClientLimiter) AllowMessage(clientID string) error {
	if cl.IsBlacklisted(clientID) { return ErrClientBlacklisted }
	limiter := cl.getOrCreateLimiter(clientID)
	if !limiter.Allow() { return ErrClientRateLimit }
	return nil
}

func (cl *ClientLimiter) Blacklist(clientID string) {
	cl.blacklist.Store(clientID, time.Now().Add(cl.config.BlacklistTTL))
}

func (cl *ClientLimiter) IsBlacklisted(clientID string) bool {
	v, ok := cl.blacklist.Load(clientID)
	if !ok { return false }
	expiry := v.(time.Time)
	if time.Now().After(expiry) {
		cl.blacklist.Delete(clientID)
		return false
	}
	return true
}

func (cl *ClientLimiter) RemoveClient(clientID string) {
	cl.limiters.Delete(clientID)
	cl.blacklist.Delete(clientID)
}

func (cl *ClientLimiter) getOrCreateLimiter(clientID string) *rate.Limiter {
	if v, ok := cl.limiters.Load(clientID); ok { return v.(*rate.Limiter) }
	limiter := rate.NewLimiter(rate.Limit(cl.config.MsgRate), cl.config.MsgBurst)
	actual, _ := cl.limiters.LoadOrStore(clientID, limiter)
	return actual.(*rate.Limiter)
}
