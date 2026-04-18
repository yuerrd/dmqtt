// Package main is an example external interceptor plugin for dmqtt.
//
// Build: go build -buildmode=plugin -o log-interceptor.so ./examples/plugins/loginterceptor/
//
// The plugin logs every PUBLISH message's topic and payload length to stdout.
package main

import (
	"context"
	"fmt"

	"github.com/langzp/dmqtt/internal/plugin"
)

// LogInterceptor logs PUBLISH events to stdout.
type LogInterceptor struct {
	level string
}

func (l *LogInterceptor) Name() string { return "log-interceptor" }
func (l *LogInterceptor) Init() error  { return nil }
func (l *LogInterceptor) Close() error { return nil }

// OnPublish logs the topic and payload length.
func (l *LogInterceptor) OnPublish(ctx context.Context, evt *plugin.PublishEvent) error {
	fmt.Printf("[%s] PUBLISH client=%s topic=%s payload_len=%d qos=%d retain=%v\n",
		l.level, evt.ClientID, evt.Topic, len(evt.Payload), evt.QoS, evt.Retain)
	return nil
}

// NewInterceptor is the exported symbol that dmqtt's PluginLoader looks up.
// config keys: "level" (default: "info")
func NewInterceptor(config map[string]string) (plugin.Interceptor, error) {
	level := "info"
	if config != nil {
		if v, ok := config["level"]; ok && v != "" {
			level = v
		}
	}
	return &LogInterceptor{level: level}, nil
}
