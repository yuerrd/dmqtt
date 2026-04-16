package logging

import (
	"log"
	"log/slog"
	"os"
	"strings"
)

// Init configures slog as the default logger with JSON output to stdout.
// Valid levels: "debug", "info", "warn", "error". Defaults to "info".
func Init(level string) {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Redirect legacy log.Printf calls through slog
	log.SetFlags(0)
	log.SetOutput(&slogWriter{logger: logger})
}

// slogWriter adapts slog.Logger to io.Writer for legacy log package.
type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if msg != "" {
		w.logger.Info(msg, "source", "legacy")
	}
	return len(p), nil
}