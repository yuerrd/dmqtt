package plugin

import (
	"testing"

	"github.com/langzp/dmqtt/config"
)

func TestPluginLoader_LoadAll_EmptyConfig(t *testing.T) {
	loader := NewPluginLoader()
	interceptors, err := loader.LoadAll(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(interceptors) != 0 {
		t.Fatalf("expected 0 interceptors, got %d", len(interceptors))
	}
}

func TestPluginLoader_LoadAll_EmptySlice(t *testing.T) {
	loader := NewPluginLoader()
	interceptors, err := loader.LoadAll([]config.PluginEntry{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(interceptors) != 0 {
		t.Fatalf("expected 0 interceptors, got %d", len(interceptors))
	}
}

func TestPluginLoader_Load_FileNotFound(t *testing.T) {
	loader := NewPluginLoader()
	_, err := loader.Load(config.PluginEntry{
		Path:   "/nonexistent/path/plugin.so",
		Config: nil,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent plugin file")
	}
	if !containsSubstring(err.Error(), "/nonexistent/path/plugin.so") {
		t.Fatalf("error should contain plugin path, got: %v", err)
	}
}

func TestPluginLoader_LoadAll_StopsOnFirstError(t *testing.T) {
	loader := NewPluginLoader()
	entries := []config.PluginEntry{
		{Path: "/nonexistent/first.so"},
		{Path: "/nonexistent/second.so"},
	}
	_, err := loader.LoadAll(entries)
	if err == nil {
		t.Fatal("expected error")
	}
	if !containsSubstring(err.Error(), "first.so") {
		t.Fatalf("error should mention first plugin, got: %v", err)
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
