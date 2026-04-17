package ratelimit

import "testing"

func TestNoopTenantLimiter_AllowsEverything(t *testing.T) {
	tl := &NoopTenantLimiter{}
	if err := tl.AllowMessage("tenant1", 100); err != nil {
		t.Fatalf("AllowMessage should succeed: %v", err)
	}
	if err := tl.AllowConnect("tenant1"); err != nil {
		t.Fatalf("AllowConnect should succeed: %v", err)
	}
}
