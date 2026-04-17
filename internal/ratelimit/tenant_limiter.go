package ratelimit

// TenantLimiter enforces per-tenant rate limits.
// This is a placeholder interface — the full implementation will come with §23 Multi-Tenancy.
type TenantLimiter interface {
	AllowMessage(tenantID string, payloadSize int) error
	AllowConnect(tenantID string) error
}

// NoopTenantLimiter allows all operations. Used when multi-tenancy is not enabled.
type NoopTenantLimiter struct{}

func (n *NoopTenantLimiter) AllowMessage(_ string, _ int) error { return nil }
func (n *NoopTenantLimiter) AllowConnect(_ string) error        { return nil }
