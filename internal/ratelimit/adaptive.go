package ratelimit

import (
	"math"
	"sync/atomic"
)

type AdaptiveController struct {
	scaleFactor atomic.Value // float64
	config      *AdaptiveConfig
}

func NewAdaptiveController(cfg *AdaptiveConfig) *AdaptiveController {
	ac := &AdaptiveController{config: cfg}
	ac.scaleFactor.Store(1.0)
	return ac
}

func (ac *AdaptiveController) ScaleFactor() float64 {
	return ac.scaleFactor.Load().(float64)
}

func (ac *AdaptiveController) UpdateLoad(load float64) {
	if !ac.config.Enabled {
		ac.scaleFactor.Store(1.0)
		return
	}
	var factor float64
	switch {
	case load > ac.config.HighLoad:
		factor = 0.5
	case load > ac.config.MediumLoad:
		factor = 0.8
	case load < ac.config.LowLoad:
		factor = 1.2
	default:
		factor = 1.0
	}
	ac.scaleFactor.Store(factor)
}

func CalcSystemLoad(cpuUsage, memUsage float64, goroutineCount int, gcPauseNs int64) float64 {
	goroutineLoad := math.Min(float64(goroutineCount)/100_000, 1.0)
	gcLoad := math.Min(float64(gcPauseNs)/1e9*10, 1.0)
	load := cpuUsage*0.4 + memUsage*0.3 + goroutineLoad*0.2 + gcLoad*0.1
	return math.Min(load, 1.0)
}
