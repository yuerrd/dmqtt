package ratelimit

import (
	"sync"
	"time"
)

type clientStats struct {
	mu            sync.Mutex
	messageCount  int64
	topicSet      map[string]struct{}
	reconnects    int64
	score         float64
	lastEvaluated time.Time
	windowStart   time.Time
}

type MaliciousDetector struct {
	stats  sync.Map
	config *DetectorConfig
}

func NewMaliciousDetector(cfg *DetectorConfig) *MaliciousDetector {
	return &MaliciousDetector{config: cfg}
}

func (d *MaliciousDetector) RecordMessage(clientID string) {
	stats := d.getOrCreate(clientID)
	stats.mu.Lock()
	stats.messageCount++
	stats.mu.Unlock()
}

func (d *MaliciousDetector) RecordSubscribe(clientID, topic string) {
	stats := d.getOrCreate(clientID)
	stats.mu.Lock()
	stats.topicSet[topic] = struct{}{}
	stats.mu.Unlock()
}

func (d *MaliciousDetector) RecordReconnect(clientID string) {
	stats := d.getOrCreate(clientID)
	stats.mu.Lock()
	stats.reconnects++
	stats.mu.Unlock()
}

func (d *MaliciousDetector) Evaluate(clientID string) {
	if !d.config.Enabled {
		return
	}
	v, ok := d.stats.Load(clientID)
	if !ok {
		return
	}
	stats := v.(*clientStats)
	stats.mu.Lock()
	defer stats.mu.Unlock()

	score := 0.0
	if float64(stats.messageCount) > d.config.HighRateThreshold {
		score += 50
	}
	if len(stats.topicSet) > d.config.ScanThreshold {
		score += 30
	}
	if int(stats.reconnects) > d.config.ReconnectThreshold {
		score += 20
	}

	stats.score = score
	stats.lastEvaluated = time.Now()
	stats.messageCount = 0
	stats.topicSet = make(map[string]struct{})
	stats.reconnects = 0
}

func (d *MaliciousDetector) Score(clientID string) float64 {
	v, ok := d.stats.Load(clientID)
	if !ok {
		return 0
	}
	stats := v.(*clientStats)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	return stats.score
}

func (d *MaliciousDetector) IsMalicious(clientID string) bool {
	if !d.config.Enabled {
		return false
	}
	return d.Score(clientID) >= d.config.ScoreThreshold
}

func (d *MaliciousDetector) Decay() {
	d.stats.Range(func(key, value any) bool {
		stats := value.(*clientStats)
		stats.mu.Lock()
		stats.score /= 2
		if stats.score < 1 {
			stats.score = 0
		}
		stats.mu.Unlock()
		return true
	})
}

func (d *MaliciousDetector) getOrCreate(clientID string) *clientStats {
	v, ok := d.stats.Load(clientID)
	if ok {
		return v.(*clientStats)
	}
	stats := &clientStats{topicSet: make(map[string]struct{}), windowStart: time.Now()}
	actual, _ := d.stats.LoadOrStore(clientID, stats)
	return actual.(*clientStats)
}
