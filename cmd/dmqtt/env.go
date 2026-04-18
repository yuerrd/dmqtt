package main

import (
	"os"

	"github.com/langzp/dmqtt/config"
)

// applyEnvOverrides reads DMQTT_* environment variables and overrides
// the corresponding config values. Empty env vars are ignored.
func applyEnvOverrides(cfg *config.Config) {
	if v := os.Getenv("DMQTT_TCP_ADDR"); v != "" {
		cfg.TCPAddr = v
	}
	if v := os.Getenv("DMQTT_TLS_ADDR"); v != "" {
		cfg.TLSAddr = v
	}
	if v := os.Getenv("DMQTT_TLS_CERT"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("DMQTT_TLS_KEY"); v != "" {
		cfg.TLSKeyFile = v
	}
	if v := os.Getenv("DMQTT_WS_ADDR"); v != "" {
		cfg.WSAddr = v
	}
	if v := os.Getenv("DMQTT_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("DMQTT_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("DMQTT_AUTH_FILE"); v != "" {
		cfg.AuthFile = v
	}
	if v := os.Getenv("DMQTT_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
}