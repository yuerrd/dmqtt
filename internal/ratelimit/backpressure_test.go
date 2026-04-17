package ratelimit

import "testing"

func TestBackpressureController_DefaultNone(t *testing.T) {
	cfg := &BackpressureConfig{Enabled: true, QueueSizeMax: 1000, CriticalThreshold: 0.9, SevereThreshold: 0.7, ModerateThreshold: 0.5}
	bp := NewBackpressureController(cfg)
	if level := bp.Level(); level != BPLevelNone {
		t.Fatalf("expected BPLevelNone, got %d", level)
	}
}

func TestBackpressureController_LevelTransitions(t *testing.T) {
	cfg := &BackpressureConfig{Enabled: true, QueueSizeMax: 100, CriticalThreshold: 0.9, SevereThreshold: 0.7, ModerateThreshold: 0.5}
	bp := NewBackpressureController(cfg)

	bp.Update(40)
	if level := bp.Level(); level != BPLevelNone {
		t.Fatalf("40%% should be None, got %d", level)
	}

	bp.Update(55)
	if level := bp.Level(); level != BPLevelModerate {
		t.Fatalf("55%% should be Moderate, got %d", level)
	}

	bp.Update(75)
	if level := bp.Level(); level != BPLevelSevere {
		t.Fatalf("75%% should be Severe, got %d", level)
	}

	bp.Update(95)
	if level := bp.Level(); level != BPLevelCritical {
		t.Fatalf("95%% should be Critical, got %d", level)
	}

	bp.Update(10)
	if level := bp.Level(); level != BPLevelNone {
		t.Fatalf("10%% should be None, got %d", level)
	}
}

func TestBackpressureController_DisabledAlwaysNone(t *testing.T) {
	cfg := &BackpressureConfig{Enabled: false, QueueSizeMax: 100, CriticalThreshold: 0.9, SevereThreshold: 0.7, ModerateThreshold: 0.5}
	bp := NewBackpressureController(cfg)
	bp.Update(95)
	if level := bp.Level(); level != BPLevelNone {
		t.Fatalf("disabled should always be None, got %d", level)
	}
}
