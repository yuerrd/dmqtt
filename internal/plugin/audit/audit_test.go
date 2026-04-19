package audit

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/yuerrd/dmqtt/internal/plugin"
)

func TestAudit_OnConnect(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()
	defer a.Close()

	err := a.OnConnect(context.Background(), &plugin.ConnectEvent{
		ClientID: "client-1",
		Username: "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for async consumer
	time.Sleep(50 * time.Millisecond)

	if !bytes.Contains(buf.Bytes(), []byte("connect")) {
		t.Fatalf("expected 'connect' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnPublish(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()
	defer a.Close()

	err := a.OnPublish(context.Background(), &plugin.PublishEvent{
		ClientID: "client-1",
		Topic:    "t/1",
		Payload:  []byte("hello"),
		QoS:      1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if !bytes.Contains(buf.Bytes(), []byte("publish")) {
		t.Fatalf("expected 'publish' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnDisconnect(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()
	defer a.Close()

	a.OnDisconnect(&plugin.DisconnectEvent{
		ClientID: "client-1",
		Reason:   "clean",
	})

	time.Sleep(50 * time.Millisecond)

	if !bytes.Contains(buf.Bytes(), []byte("disconnect")) {
		t.Fatalf("expected 'disconnect' in log output, got: %s", buf.String())
	}
}

func TestAudit_BufferFull_Drops(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	// Use buffer size of 1 and don't start consumer to force drops
	a := &AuditInterceptor{
		entries: make(chan *AuditEntry, 1),
		logger:  logger,
		done:    make(chan struct{}),
	}
	// Don't call Init() — no consumer goroutine

	// Fill the buffer
	a.entries <- &AuditEntry{Action: "test"}

	// This should be dropped (buffer full, no backup path)
	a.record(&AuditEntry{Action: "dropped"})

	// Verify the buffer still has just 1 entry
	if len(a.entries) != 1 {
		t.Fatalf("expected buffer size 1, got %d", len(a.entries))
	}
}
