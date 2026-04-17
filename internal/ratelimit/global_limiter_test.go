package ratelimit

import "testing"

func TestGlobalLimiter_AllowsWithinRate(t *testing.T) {
	cfg := &GlobalConfig{IngressRate: 100, IngressBurst: 200, ConnectRate: 10, ConnectBurst: 20}
	gl := NewGlobalLimiter(cfg)
	for i := 0; i < 100; i++ {
		if err := gl.AllowIngress(); err != nil {
			t.Fatalf("AllowIngress should succeed at i=%d: %v", i, err)
		}
	}
}

func TestGlobalLimiter_RejectsOverBurst(t *testing.T) {
	cfg := &GlobalConfig{IngressRate: 1, IngressBurst: 5, ConnectRate: 10, ConnectBurst: 20}
	gl := NewGlobalLimiter(cfg)
	rejected := 0
	for i := 0; i < 20; i++ {
		if err := gl.AllowIngress(); err != nil { rejected++ }
	}
	if rejected == 0 { t.Fatal("expected some rejections when exceeding burst") }
}

func TestGlobalLimiter_ConnectRateLimit(t *testing.T) {
	cfg := &GlobalConfig{IngressRate: 1000, IngressBurst: 2000, ConnectRate: 1, ConnectBurst: 3}
	gl := NewGlobalLimiter(cfg)
	rejected := 0
	for i := 0; i < 10; i++ {
		if err := gl.AllowConnect(); err != nil { rejected++ }
	}
	if rejected == 0 { t.Fatal("expected some connect rejections") }
}
