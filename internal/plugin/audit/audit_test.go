package audit

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yuerrd/dmqtt/internal/plugin"
)

func TestAudit_OnConnect(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

	err := a.OnConnect(context.Background(), &plugin.ConnectEvent{
		ClientID: "client-1",
		Username: "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for async consumer then close to stop writes before reading
	time.Sleep(50 * time.Millisecond)
	a.Close()

	if !bytes.Contains(buf.Bytes(), []byte("connect")) {
		t.Fatalf("expected 'connect' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnPublish(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

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
	a.Close()

	if !bytes.Contains(buf.Bytes(), []byte("publish")) {
		t.Fatalf("expected 'publish' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnDisconnect(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

	a.OnDisconnect(&plugin.DisconnectEvent{
		ClientID: "client-1",
		Reason:   "clean",
	})

	time.Sleep(50 * time.Millisecond)
	a.Close()

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
		stopped: make(chan struct{}),
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

func TestAudit_Name(t *testing.T) {
	a := New(Config{}, nil)
	if a.Name() != "audit" {
		t.Errorf("Name() = %q, want %q", a.Name(), "audit")
	}
}

func TestAudit_OnSubscribe(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

	err := a.OnSubscribe(context.Background(), &plugin.SubscribeEvent{
		ClientID:    "client-1",
		TopicFilter: "sensor/#",
		QoS:         1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	a.Close()

	if !bytes.Contains(buf.Bytes(), []byte("subscribe")) {
		t.Fatalf("expected 'subscribe' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnDelivery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

	err := a.OnDelivery(context.Background(), &plugin.DeliveryEvent{
		ClientID: "client-1",
		Topic:    "sensor/temp",
		Payload:  []byte("25.5"),
		QoS:      0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	a.Close()

	if !bytes.Contains(buf.Bytes(), []byte("delivery")) {
		t.Fatalf("expected 'delivery' in log output, got: %s", buf.String())
	}
}

func TestAudit_OnSessionExpired(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(Config{BufferSize: 100}, logger)
	a.Init()

	a.OnSessionExpired(&plugin.SessionExpiredEvent{ClientID: "client-1"})

	time.Sleep(50 * time.Millisecond)
	a.Close()

	if !bytes.Contains(buf.Bytes(), []byte("session_expired")) {
		t.Fatalf("expected 'session_expired' in log output, got: %s", buf.String())
	}
}

func TestAudit_WriteToBackup(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "audit-backup-*.ndjson")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	a := &AuditInterceptor{
		entries:    make(chan *AuditEntry, 1),
		backupPath: tmpFile.Name(),
		logger:     slog.Default(),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}

	// Fill the channel so the next record goes to backup
	a.entries <- &AuditEntry{Action: "filler"}

	a.record(&AuditEntry{Action: "backup-entry", ClientID: "client-x"})

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Contains(content, []byte("backup-entry")) {
		t.Errorf("expected 'backup-entry' in backup file, got: %s", content)
	}
}

func TestAudit_DefaultBufferSize(t *testing.T) {
	// BufferSize <= 0 should default to 4096
	a := New(Config{BufferSize: 0}, nil)
	a.Init()
	defer a.Close()

	if cap(a.entries) != 4096 {
		t.Errorf("buffer size = %d, want 4096", cap(a.entries))
	}
}
