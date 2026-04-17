package tenant

import (
	"testing"
)

func TestManager_LoadAndTryConnect(t *testing.T) {
	m := NewManager()
	err := m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 2
    max_message_rate: 100
    max_message_size: 1024
`))
	if err != nil {
		t.Fatal(err)
	}
	if m.TenantCount() != 1 {
		t.Fatalf("expected 1 tenant, got %d", m.TenantCount())
	}

	// First two connections succeed
	if err := m.TryConnect("client1", "acme"); err != nil {
		t.Fatalf("TryConnect client1: %v", err)
	}
	if err := m.TryConnect("client2", "acme"); err != nil {
		t.Fatalf("TryConnect client2: %v", err)
	}

	// Third connection rejected
	if err := m.TryConnect("client3", "acme"); err == nil {
		t.Fatal("expected quota error for client3")
	}

	// Disconnect one, then third succeeds
	m.OnDisconnect("client1")
	if err := m.TryConnect("client3", "acme"); err != nil {
		t.Fatalf("TryConnect client3 after disconnect: %v", err)
	}
}

func TestManager_DefaultTenant(t *testing.T) {
	m := NewManager()
	// No tenants loaded — empty tenantID should use default (unlimited)
	if err := m.TryConnect("client1", ""); err != nil {
		t.Fatalf("default tenant should allow connection: %v", err)
	}
	prefix := m.GetTopicPrefix("client1")
	if prefix != "" {
		t.Errorf("default tenant should have empty prefix, got %q", prefix)
	}
}

func TestManager_CheckPublish_RateLimit(t *testing.T) {
	m := NewManager()
	err := m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1
    max_message_size: 1024
`))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TryConnect("client1", "acme"); err != nil {
		t.Fatal(err)
	}

	// First message should succeed (burst=1)
	if err := m.CheckPublish("client1", 100); err != nil {
		t.Fatalf("first publish should succeed: %v", err)
	}

	// Second immediate message should be rate limited
	if err := m.CheckPublish("client1", 100); err == nil {
		t.Fatal("expected rate limit error")
	}
}

func TestManager_CheckPublish_MessageSize(t *testing.T) {
	m := NewManager()
	err := m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 100
`))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TryConnect("client1", "acme"); err != nil {
		t.Fatal(err)
	}

	if err := m.CheckPublish("client1", 50); err != nil {
		t.Fatalf("small message should succeed: %v", err)
	}
	if err := m.CheckPublish("client1", 200); err == nil {
		t.Fatal("expected message size error")
	}
}

func TestManager_GetTopicPrefix(t *testing.T) {
	m := NewManager()
	err := m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 100
    max_message_size: 1024
    topic_prefix: custom-prefix
`))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TryConnect("client1", "acme"); err != nil {
		t.Fatal(err)
	}
	prefix := m.GetTopicPrefix("client1")
	if prefix != "custom-prefix" {
		t.Errorf("expected prefix=custom-prefix, got %q", prefix)
	}
}

func TestManager_UnknownTenant_FallsToDefault(t *testing.T) {
	m := NewManager()
	// Connect with a tenant that doesn't exist in config
	if err := m.TryConnect("client1", "nonexistent"); err != nil {
		t.Fatalf("unknown tenant should fall to default: %v", err)
	}
	prefix := m.GetTopicPrefix("client1")
	if prefix != "" {
		t.Errorf("unknown tenant should have empty prefix, got %q", prefix)
	}
}

func TestManager_Reload(t *testing.T) {
	m := NewManager()
	err := m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 1
    max_message_rate: 100
    max_message_size: 1024
`))
	if err != nil {
		t.Fatal(err)
	}

	// Connect one client
	if err := m.TryConnect("client1", "acme"); err != nil {
		t.Fatal(err)
	}

	// Second should fail with limit=1
	if err := m.TryConnect("client2", "acme"); err == nil {
		t.Fatal("expected quota error")
	}

	// Reload with higher limit
	err = m.LoadTenantsFromBytes([]byte(`
tenants:
  - tenant_id: acme
    max_connections: 5
    max_message_rate: 100
    max_message_size: 1024
`))
	if err != nil {
		t.Fatal(err)
	}

	// Now second should succeed
	if err := m.TryConnect("client2", "acme"); err != nil {
		t.Fatalf("after reload should allow: %v", err)
	}
}
