package main

import (
	"context"
	"testing"

	"github.com/yuerrd/dmqtt/internal/plugin"
)

func TestNewInterceptor_DefaultLevel(t *testing.T) {
	i, err := NewInterceptor(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if i.Name() != "log-interceptor" {
		t.Fatalf("expected name 'log-interceptor', got %q", i.Name())
	}
	li := i.(*LogInterceptor)
	if li.level != "info" {
		t.Fatalf("expected default level 'info', got %q", li.level)
	}
}

func TestNewInterceptor_CustomConfig(t *testing.T) {
	cfg := map[string]string{"level": "debug"}
	i, err := NewInterceptor(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	li := i.(*LogInterceptor)
	if li.level != "debug" {
		t.Fatalf("expected level 'debug', got %q", li.level)
	}
}

func TestLogInterceptor_InitClose(t *testing.T) {
	i, _ := NewInterceptor(nil)
	if err := i.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if err := i.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestLogInterceptor_OnPublish(t *testing.T) {
	i, _ := NewInterceptor(map[string]string{"level": "debug"})
	li := i.(*LogInterceptor)
	evt := &plugin.PublishEvent{
		ClientID: "client-1",
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		Retain:   false,
	}
	err := li.OnPublish(context.Background(), evt)
	if err != nil {
		t.Fatalf("OnPublish() error: %v", err)
	}
}
