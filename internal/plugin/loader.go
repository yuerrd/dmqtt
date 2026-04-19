package plugin

import (
	"fmt"
	"plugin"

	"github.com/yuerrd/dmqtt/config"
)

// PluginLoader loads external Interceptor plugins from .so shared libraries.
type PluginLoader struct{}

// NewPluginLoader creates a new PluginLoader.
func NewPluginLoader() *PluginLoader {
	return &PluginLoader{}
}

// LoadAll loads all configured external plugins. Returns error on first failure (fail-fast).
func (l *PluginLoader) LoadAll(entries []config.PluginEntry) ([]Interceptor, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	var result []Interceptor
	for _, entry := range entries {
		interceptor, err := l.Load(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, interceptor)
	}
	return result, nil
}

// Load opens a single .so file and returns the Interceptor it exports.
//
// The .so must export a symbol named "NewInterceptor" with signature:
//
//	func NewInterceptor(config map[string]string) (Interceptor, error)
func (l *PluginLoader) Load(entry config.PluginEntry) (Interceptor, error) {
	p, err := plugin.Open(entry.Path)
	if err != nil {
		return nil, fmt.Errorf("plugin load %q: %w", entry.Path, err)
	}

	sym, err := p.Lookup("NewInterceptor")
	if err != nil {
		return nil, fmt.Errorf("plugin %q: symbol NewInterceptor not found: %w", entry.Path, err)
	}

	constructor, ok := sym.(func(map[string]string) (Interceptor, error))
	if !ok {
		return nil, fmt.Errorf("plugin %q: NewInterceptor has wrong signature, expected func(map[string]string) (Interceptor, error)", entry.Path)
	}

	interceptor, err := constructor(entry.Config)
	if err != nil {
		return nil, fmt.Errorf("plugin %q: NewInterceptor returned error: %w", entry.Path, err)
	}

	return interceptor, nil
}
