package main

import (
	"os"
	"strconv"
	"strings"

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

	// Cluster overrides
	if v := os.Getenv("DMQTT_CLUSTER_ENABLED"); v != "" {
		cfg.Cluster.Enabled = strings.EqualFold(v, "true")
	}
	if v := os.Getenv("DMQTT_CLUSTER_NODE_ID"); v != "" {
		cfg.Cluster.NodeID = v
	}
	if v := os.Getenv("DMQTT_CLUSTER_HOST"); v != "" {
		cfg.Cluster.Host = v
	}
	if v := os.Getenv("DMQTT_CLUSTER_GOSSIP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Cluster.GossipPort = port
		}
	}
	if v := os.Getenv("DMQTT_CLUSTER_SEEDS"); v != "" {
		cfg.Cluster.Seeds = strings.Split(v, ",")
	}
}
