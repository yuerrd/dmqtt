package main

import (
	"os"
	"testing"

	"github.com/langzp/dmqtt/config"
)

func TestApplyEnvOverrides_Defaults(t *testing.T) {
	// No env vars set — config should stay at defaults
	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	if cfg.TCPAddr != ":1883" {
		t.Errorf("TCPAddr = %q, want %q", cfg.TCPAddr, ":1883")
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9090")
	}
	if cfg.DataDir != "" {
		t.Errorf("DataDir = %q, want empty", cfg.DataDir)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
}

func TestApplyEnvOverrides_AllVars(t *testing.T) {
	envs := map[string]string{
		"DMQTT_TCP_ADDR":  ":2883",
		"DMQTT_TLS_ADDR":  ":8883",
		"DMQTT_TLS_CERT":  "/tmp/cert.pem",
		"DMQTT_TLS_KEY":   "/tmp/key.pem",
		"DMQTT_WS_ADDR":   ":8083",
		"DMQTT_HTTP_ADDR": ":8080",
		"DMQTT_DATA_DIR":  "/opt/dmqtt/data",
		"DMQTT_AUTH_FILE":  "/etc/dmqtt/auth.conf",
		"DMQTT_LOG_LEVEL": "debug",
	}
	for k, v := range envs {
		os.Setenv(k, v)
	}
	defer func() {
		for k := range envs {
			os.Unsetenv(k)
		}
	}()

	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	if cfg.TCPAddr != ":2883" {
		t.Errorf("TCPAddr = %q, want %q", cfg.TCPAddr, ":2883")
	}
	if cfg.TLSAddr != ":8883" {
		t.Errorf("TLSAddr = %q, want %q", cfg.TLSAddr, ":8883")
	}
	if cfg.TLSCertFile != "/tmp/cert.pem" {
		t.Errorf("TLSCertFile = %q, want %q", cfg.TLSCertFile, "/tmp/cert.pem")
	}
	if cfg.TLSKeyFile != "/tmp/key.pem" {
		t.Errorf("TLSKeyFile = %q, want %q", cfg.TLSKeyFile, "/tmp/key.pem")
	}
	if cfg.WSAddr != ":8083" {
		t.Errorf("WSAddr = %q, want %q", cfg.WSAddr, ":8083")
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.DataDir != "/opt/dmqtt/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/opt/dmqtt/data")
	}
	if cfg.AuthFile != "/etc/dmqtt/auth.conf" {
		t.Errorf("AuthFile = %q, want %q", cfg.AuthFile, "/etc/dmqtt/auth.conf")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
}

func TestApplyEnvOverrides_PartialOverride(t *testing.T) {
	os.Setenv("DMQTT_DATA_DIR", "/data/mqtt")
	defer os.Unsetenv("DMQTT_DATA_DIR")

	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	if cfg.DataDir != "/data/mqtt" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/data/mqtt")
	}
	// Other fields unchanged
	if cfg.TCPAddr != ":1883" {
		t.Errorf("TCPAddr = %q, want %q (unchanged)", cfg.TCPAddr, ":1883")
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q (unchanged)", cfg.HTTPAddr, ":9090")
	}
}