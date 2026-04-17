package ratelimit

import "testing"

func TestAdaptiveController_DefaultScaleFactor(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: true, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	if sf := ac.ScaleFactor(); sf != 1.0 { t.Fatalf("default should be 1.0, got %f", sf) }
}

func TestAdaptiveController_HighLoadReduces(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: true, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	ac.UpdateLoad(0.95)
	if sf := ac.ScaleFactor(); sf != 0.5 { t.Fatalf("high load should set 0.5, got %f", sf) }
}

func TestAdaptiveController_MediumLoadReduces(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: true, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	ac.UpdateLoad(0.75)
	if sf := ac.ScaleFactor(); sf != 0.8 { t.Fatalf("medium load should set 0.8, got %f", sf) }
}

func TestAdaptiveController_LowLoadIncreases(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: true, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	ac.UpdateLoad(0.2)
	if sf := ac.ScaleFactor(); sf != 1.2 { t.Fatalf("low load should set 1.2, got %f", sf) }
}

func TestAdaptiveController_NormalLoad(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: true, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	ac.UpdateLoad(0.5)
	if sf := ac.ScaleFactor(); sf != 1.0 { t.Fatalf("normal load should be 1.0, got %f", sf) }
}

func TestAdaptiveController_DisabledAlwaysOne(t *testing.T) {
	cfg := &AdaptiveConfig{Enabled: false, HighLoad: 0.9, MediumLoad: 0.7, LowLoad: 0.3}
	ac := NewAdaptiveController(cfg)
	ac.UpdateLoad(0.95)
	if sf := ac.ScaleFactor(); sf != 1.0 { t.Fatalf("disabled should be 1.0, got %f", sf) }
}

func TestCalcSystemLoad(t *testing.T) {
	load := CalcSystemLoad(0.5, 0.3, 50000, 100000000)
	if load < 0 || load > 1 { t.Fatalf("load should be between 0 and 1, got %f", load) }
}
