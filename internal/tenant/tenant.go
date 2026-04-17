package tenant

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Tenant defines a tenant's resource limits and namespace configuration.
type Tenant struct {
	ID             string `yaml:"tenant_id"`
	MaxConnections int    `yaml:"max_connections"`
	MaxMessageRate int    `yaml:"max_message_rate"`
	MaxMessageSize int    `yaml:"max_message_size"`
	TopicPrefix    string `yaml:"topic_prefix"`
}

type tenantsFile struct {
	Tenants []Tenant `yaml:"tenants"`
}

// ParseTenants parses tenant definitions from YAML bytes.
func ParseTenants(data []byte) ([]Tenant, error) {
	var f tenantsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("failed to parse tenants YAML: %w", err)
	}

	seen := make(map[string]bool)
	for i := range f.Tenants {
		t := &f.Tenants[i]
		if t.ID == "" {
			return nil, fmt.Errorf("tenant_id cannot be empty")
		}
		if seen[t.ID] {
			return nil, fmt.Errorf("duplicate tenant_id: %s", t.ID)
		}
		seen[t.ID] = true
		if t.TopicPrefix == "" {
			t.TopicPrefix = t.ID
		}
	}

	return f.Tenants, nil
}

// LoadTenantsFromFile loads tenant definitions from a YAML file.
func LoadTenantsFromFile(path string) ([]Tenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read tenants file: %w", err)
	}
	return ParseTenants(data)
}
