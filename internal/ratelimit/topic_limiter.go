package ratelimit

import (
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
	"time"
)

type TopicLimiter struct {
	limiters sync.Map
	counters sync.Map
	config   *TopicConfig
}

type topicCounter struct {
	count     atomic.Int64
	windowEnd atomic.Int64
}

func NewTopicLimiter(cfg *TopicConfig) *TopicLimiter {
	return &TopicLimiter{config: cfg}
}

func (tl *TopicLimiter) AllowPublish(topic string) error {
	if entry, ok := tl.config.TopicConfigs[topic]; ok {
		limiter := tl.getOrCreateLimiter(topic, entry.MaxPublishRate, entry.MaxBurst)
		if !limiter.Allow() {
			return ErrTopicRateLimit
		}
		return nil
	}
	if tl.config.DefaultRate > 0 {
		limiter := tl.getOrCreateLimiter(topic, tl.config.DefaultRate, tl.config.DefaultBurst)
		if !limiter.Allow() {
			return ErrTopicRateLimit
		}
	}
	return nil
}

func (tl *TopicLimiter) RecordPublish(topic string) {
	now := time.Now().UnixNano()
	windowEnd := now + tl.config.HotspotWindow.Nanoseconds()
	v, loaded := tl.counters.LoadOrStore(topic, &topicCounter{})
	counter := v.(*topicCounter)
	if !loaded {
		counter.windowEnd.Store(windowEnd)
	}
	if now > counter.windowEnd.Load() {
		counter.count.Store(0)
		counter.windowEnd.Store(windowEnd)
	}
	counter.count.Add(1)
}

func (tl *TopicLimiter) IsHotspot(topic string) bool {
	v, ok := tl.counters.Load(topic)
	if !ok {
		return false
	}
	counter := v.(*topicCounter)
	return float64(counter.count.Load()) > tl.config.HotspotThreshold
}

func (tl *TopicLimiter) getOrCreateLimiter(topic string, r float64, burst int) *rate.Limiter {
	if v, ok := tl.limiters.Load(topic); ok {
		return v.(*rate.Limiter)
	}
	limiter := rate.NewLimiter(rate.Limit(r), burst)
	actual, _ := tl.limiters.LoadOrStore(topic, limiter)
	return actual.(*rate.Limiter)
}
