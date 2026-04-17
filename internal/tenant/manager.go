package tenant

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"
)

// TenantManager manages tenant runtime state: connection counts, rate limiters, client mappings.
type TenantManager struct {
	mu      sync.RWMutex
	tenants map[string]*tenantState // tenant_id → state
	clients sync.Map                // clientID → string (tenant_id)
}

type tenantState struct {
	config      Tenant
	connections atomic.Int64
	rateLimiter *rate.Limiter
}

// defaultTenant is the built-in tenant for users without a tenant_id.
var defaultTenant = &tenantState{
	config: Tenant{
		ID:             "",
		MaxConnections: 0,
		MaxMessageRate: 0,
		MaxMessageSize: 0,
		TopicPrefix:    "",
	},
	rateLimiter: rate.NewLimiter(rate.Inf, 0),
}

// NewManager creates a new TenantManager with no tenants loaded.
func NewManager() *TenantManager {
	return &TenantManager{
		tenants: make(map[string]*tenantState),
	}
}

// LoadTenantsFromBytes parses and loads tenant definitions from YAML bytes.
// Existing connections are preserved; only config and rate limiters are updated.
func (m *TenantManager) LoadTenantsFromBytes(data []byte) error {
	parsed, err := ParseTenants(data)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	newTenants := make(map[string]*tenantState, len(parsed))
	for _, t := range parsed {
		existing, ok := m.tenants[t.ID]
		if ok {
			existing.config = t
			if t.MaxMessageRate > 0 {
				existing.rateLimiter = rate.NewLimiter(rate.Limit(t.MaxMessageRate), t.MaxMessageRate)
			} else {
				existing.rateLimiter = rate.NewLimiter(rate.Inf, 0)
			}
			newTenants[t.ID] = existing
		} else {
			var rl *rate.Limiter
			if t.MaxMessageRate > 0 {
				rl = rate.NewLimiter(rate.Limit(t.MaxMessageRate), t.MaxMessageRate)
			} else {
				rl = rate.NewLimiter(rate.Inf, 0)
			}
			newTenants[t.ID] = &tenantState{
				config:      t,
				rateLimiter: rl,
			}
		}
	}

	m.tenants = newTenants
	return nil
}

// LoadTenants loads tenant definitions from a YAML file.
func (m *TenantManager) LoadTenants(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read tenants file: %w", err)
	}
	return m.LoadTenantsFromBytes(data)
}

// TenantCount returns the number of loaded tenants.
func (m *TenantManager) TenantCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tenants)
}

// TryConnect checks the tenant's connection quota and records the client mapping.
func (m *TenantManager) TryConnect(clientID, tenantID string) error {
	state := m.getState(tenantID)

	if state.config.MaxConnections > 0 {
		current := state.connections.Load()
		if current >= int64(state.config.MaxConnections) {
			return fmt.Errorf("tenant %q connection quota exhausted (%d/%d)",
				effectiveTenantID(tenantID), current, state.config.MaxConnections)
		}
	}

	state.connections.Add(1)
	m.clients.Store(clientID, tenantID)
	return nil
}

// OnDisconnect releases the connection count and cleans up the client mapping.
func (m *TenantManager) OnDisconnect(clientID string) {
	val, ok := m.clients.LoadAndDelete(clientID)
	if !ok {
		return
	}
	tenantID := val.(string)
	state := m.getState(tenantID)
	state.connections.Add(-1)
}

// CheckPublish checks rate limit and message size for the client's tenant.
func (m *TenantManager) CheckPublish(clientID string, payloadSize int) error {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return nil
	}
	tenantID := val.(string)
	state := m.getState(tenantID)

	if state.config.MaxMessageSize > 0 && payloadSize > state.config.MaxMessageSize {
		return fmt.Errorf("tenant %q message size %d exceeds limit %d",
			effectiveTenantID(tenantID), payloadSize, state.config.MaxMessageSize)
	}

	if !state.rateLimiter.Allow() {
		return fmt.Errorf("tenant %q message rate limit exceeded",
			effectiveTenantID(tenantID))
	}

	return nil
}

// GetTopicPrefix returns the topic prefix for the given client.
func (m *TenantManager) GetTopicPrefix(clientID string) string {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return ""
	}
	tenantID := val.(string)
	state := m.getState(tenantID)
	return state.config.TopicPrefix
}

// GetTenantID returns the tenant ID for the given client.
func (m *TenantManager) GetTenantID(clientID string) string {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return ""
	}
	return val.(string)
}

// ActiveConnections returns the current connection count for a tenant.
func (m *TenantManager) ActiveConnections(tenantID string) int64 {
	state := m.getState(tenantID)
	return state.connections.Load()
}

func (m *TenantManager) getState(tenantID string) *tenantState {
	if tenantID == "" {
		return defaultTenant
	}
	m.mu.RLock()
	state, ok := m.tenants[tenantID]
	m.mu.RUnlock()
	if !ok {
		return defaultTenant
	}
	return state
}

func effectiveTenantID(tenantID string) string {
	if tenantID == "" {
		return "default"
	}
	return tenantID
}
