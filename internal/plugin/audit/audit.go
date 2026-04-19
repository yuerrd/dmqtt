package audit

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/yuerrd/dmqtt/internal/metrics"
	"github.com/yuerrd/dmqtt/internal/plugin"
)

// Config configures the audit interceptor.
type Config struct {
	BufferSize int
	BackupPath string
}

// AuditEntry represents a single audit log entry.
type AuditEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Action    string         `json:"action"`
	ClientID  string         `json:"client_id"`
	Details   map[string]any `json:"details,omitempty"`
}

// AuditInterceptor logs MQTT events for auditing.
type AuditInterceptor struct {
	entries    chan *AuditEntry
	backupPath string
	logger     *slog.Logger
	done       chan struct{}
	closeOnce  sync.Once
}

// New creates a new AuditInterceptor.
func New(cfg Config, logger *slog.Logger) *AuditInterceptor {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 4096
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditInterceptor{
		entries:    make(chan *AuditEntry, cfg.BufferSize),
		backupPath: cfg.BackupPath,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

func (a *AuditInterceptor) Name() string { return "audit" }

func (a *AuditInterceptor) Init() error {
	go a.consumeLoop()
	return nil
}

func (a *AuditInterceptor) Close() error {
	a.closeOnce.Do(func() {
		close(a.done)
	})
	// Drain remaining entries after consumeLoop exits
	for {
		select {
		case entry := <-a.entries:
			a.writeEntry(entry)
		default:
			return nil
		}
	}
}

func (a *AuditInterceptor) consumeLoop() {
	for {
		select {
		case entry := <-a.entries:
			a.writeEntry(entry)
		case <-a.done:
			return
		}
	}
}

func (a *AuditInterceptor) writeEntry(entry *AuditEntry) {
	a.logger.Info("audit",
		"action", entry.Action,
		"client_id", entry.ClientID,
		"timestamp", entry.Timestamp,
		"details", entry.Details,
	)
}

func (a *AuditInterceptor) record(entry *AuditEntry) {
	entry.Timestamp = time.Now()
	metrics.AuditEntry(entry.Action)

	select {
	case a.entries <- entry:
	default:
		// Buffer full — try backup file
		if a.backupPath != "" {
			a.writeToBackup(entry)
		} else {
			metrics.AuditDropped()
		}
	}
}

func (a *AuditInterceptor) writeToBackup(entry *AuditEntry) {
	f, err := os.OpenFile(a.backupPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		metrics.AuditDropped()
		return
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		metrics.AuditDropped()
		return
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		metrics.AuditDropped()
	}
}

// --- Hook implementations ---

func (a *AuditInterceptor) OnConnect(_ context.Context, evt *plugin.ConnectEvent) error {
	a.record(&AuditEntry{
		Action:   "connect",
		ClientID: evt.ClientID,
		Details: map[string]any{
			"username":      evt.Username,
			"clean_session": evt.CleanSession,
			"remote_addr":   evt.RemoteAddr,
		},
	})
	return nil
}

func (a *AuditInterceptor) OnPublish(_ context.Context, evt *plugin.PublishEvent) error {
	a.record(&AuditEntry{
		Action:   "publish",
		ClientID: evt.ClientID,
		Details: map[string]any{
			"topic":        evt.Topic,
			"qos":          evt.QoS,
			"retain":       evt.Retain,
			"payload_size": len(evt.Payload),
		},
	})
	return nil
}

func (a *AuditInterceptor) OnSubscribe(_ context.Context, evt *plugin.SubscribeEvent) error {
	a.record(&AuditEntry{
		Action:   "subscribe",
		ClientID: evt.ClientID,
		Details: map[string]any{
			"topic_filter": evt.TopicFilter,
			"qos":          evt.QoS,
		},
	})
	return nil
}

func (a *AuditInterceptor) OnDelivery(_ context.Context, evt *plugin.DeliveryEvent) error {
	a.record(&AuditEntry{
		Action:   "delivery",
		ClientID: evt.ClientID,
		Details: map[string]any{
			"topic":        evt.Topic,
			"qos":          evt.QoS,
			"payload_size": len(evt.Payload),
		},
	})
	return nil
}

func (a *AuditInterceptor) OnDisconnect(evt *plugin.DisconnectEvent) {
	a.record(&AuditEntry{
		Action:   "disconnect",
		ClientID: evt.ClientID,
		Details: map[string]any{
			"reason":      evt.Reason,
			"remote_addr": evt.RemoteAddr,
		},
	})
}

func (a *AuditInterceptor) OnSessionExpired(evt *plugin.SessionExpiredEvent) {
	a.record(&AuditEntry{
		Action:   "session_expired",
		ClientID: evt.ClientID,
	})
}
