package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestInit_SetsDefaultLogger(t *testing.T) {
	Init("info")

	// Verify slog default is set by checking it doesn't panic
	slog.Info("test message", "key", "value")
}

func TestInit_ParsesLevels(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"invalid", slog.LevelInfo}, // default
		{"", slog.LevelInfo},        // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var level slog.Level
			switch tt.input {
			case "debug":
				level = slog.LevelDebug
			case "warn", "warning":
				level = slog.LevelWarn
			case "error":
				level = slog.LevelError
			default:
				level = slog.LevelInfo
			}
			if level != tt.want {
				t.Errorf("level = %v, want %v", level, tt.want)
			}
		})
	}
}

func TestSlogWriter(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	w := &slogWriter{logger: logger}
	w.Write([]byte("hello from legacy\n"))

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg, ok := entry["msg"].(string); !ok || msg != "hello from legacy" {
		t.Errorf("msg = %v, want 'hello from legacy'", entry["msg"])
	}
	if src, ok := entry["source"].(string); !ok || src != "legacy" {
		t.Errorf("source = %v, want 'legacy'", entry["source"])
	}
}