package tenant

import (
	"os"
	"testing"
)

func TestParseTenants(t *testing.T) {
	yaml := []byte(`
tenants:
  - tenant_id: acme
    max_connections: 100
    max_message_rate: 1000
    max_message_size: 65536
    topic_prefix: "acme"
  - tenant_id: demo
    max_connections: 10
    max_message_rate: 100
    max_message_size: 32768
`)
	tenants, err := ParseTenants(yaml)
	if err != nil {
		t.Fatalf("ParseTenants: %v", err)
	}
	if len(tenants) != 2 {
		t.Fatalf("expected 2 tenants, got %d", len(tenants))
	}
	if tenants[0].ID != "acme" {
		t.Errorf("expected tenant_id=acme, got %s", tenants[0].ID)
	}
	if tenants[0].MaxConnections != 100 {
		t.Errorf("expected max_connections=100, got %d", tenants[0].MaxConnections)
	}
	if tenants[0].MaxMessageRate != 1000 {
		t.Errorf("expected max_message_rate=1000, got %d", tenants[0].MaxMessageRate)
	}
	if tenants[0].MaxMessageSize != 65536 {
		t.Errorf("expected max_message_size=65536, got %d", tenants[0].MaxMessageSize)
	}
	if tenants[0].TopicPrefix != "acme" {
		t.Errorf("expected topic_prefix=acme, got %s", tenants[0].TopicPrefix)
	}
	if tenants[1].ID != "demo" {
		t.Errorf("expected tenant_id=demo, got %s", tenants[1].ID)
	}
}

func TestParseTenants_DefaultPrefix(t *testing.T) {
	yaml := []byte(`
tenants:
  - tenant_id: acme
    max_connections: 50
    max_message_rate: 500
    max_message_size: 32768
`)
	tenants, err := ParseTenants(yaml)
	if err != nil {
		t.Fatalf("ParseTenants: %v", err)
	}
	if tenants[0].TopicPrefix != "acme" {
		t.Errorf("expected default prefix=acme, got %s", tenants[0].TopicPrefix)
	}
}

func TestParseTenants_EmptyID(t *testing.T) {
	yaml := []byte(`
tenants:
  - tenant_id: ""
    max_connections: 10
    max_message_rate: 100
    max_message_size: 32768
`)
	_, err := ParseTenants(yaml)
	if err == nil {
		t.Fatal("expected error for empty tenant_id")
	}
}

func TestParseTenants_DuplicateID(t *testing.T) {
	yaml := []byte(`
tenants:
  - tenant_id: acme
    max_connections: 100
    max_message_rate: 1000
    max_message_size: 65536
  - tenant_id: acme
    max_connections: 50
    max_message_rate: 500
    max_message_size: 32768
`)
	_, err := ParseTenants(yaml)
	if err == nil {
		t.Fatal("expected error for duplicate tenant_id")
	}
}

func TestLoadTenantsFromFile(t *testing.T) {
	path := t.TempDir() + "/tenants.yaml"
	data := []byte(`
tenants:
  - tenant_id: test
    max_connections: 5
    max_message_rate: 50
    max_message_size: 1024
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	tenants, err := LoadTenantsFromFile(path)
	if err != nil {
		t.Fatalf("LoadTenantsFromFile: %v", err)
	}
	if len(tenants) != 1 || tenants[0].ID != "test" {
		t.Errorf("unexpected tenants: %+v", tenants)
	}
}
