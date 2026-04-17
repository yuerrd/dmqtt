package ratelimit

import "sync/atomic"

type BackpressureController struct {
	level  atomic.Int32
	config *BackpressureConfig
}

func NewBackpressureController(cfg *BackpressureConfig) *BackpressureController {
	return &BackpressureController{config: cfg}
}

func (bp *BackpressureController) Level() BackpressureLevel {
	return BackpressureLevel(bp.level.Load())
}

func (bp *BackpressureController) Update(currentQueueSize int) {
	if !bp.config.Enabled {
		bp.level.Store(int32(BPLevelNone))
		return
	}
	ratio := float64(currentQueueSize) / float64(bp.config.QueueSizeMax)
	var level BackpressureLevel
	switch {
	case ratio >= bp.config.CriticalThreshold:
		level = BPLevelCritical
	case ratio >= bp.config.SevereThreshold:
		level = BPLevelSevere
	case ratio >= bp.config.ModerateThreshold:
		level = BPLevelModerate
	default:
		level = BPLevelNone
	}
	bp.level.Store(int32(level))
}
