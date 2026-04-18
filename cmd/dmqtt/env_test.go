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

func TestApplyEnvOverrides_ClusterVars(t *testing.T) {
	envs := map[string]string{
		"DMQTT_CLUSTER_ENABLED":     "true",
		"DMQTT_CLUSTER_NODE_ID":     "node-0",
		"DMQTT_CLUSTER_HOST":        "10.0.0.1",
		"DMQTT_CLUSTER_GOSSIP_PORT": "7100",
		"DMQTT_CLUSTER_SEEDS":       "10.0.0.2:7100,10.0.0.3:7100",
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

	if !cfg.Cluster.Enabled {
		t.Error("Cluster.Enabled = false, want true")
	}
	if cfg.Cluster.NodeID != "node-0" {
		t.Errorf("Cluster.NodeID = %q, want %q", cfg.Cluster.NodeID, "node-0")
	}
	if cfg.Cluster.Host != "10.0.0.1" {
		t.Errorf("Cluster.Host = %q, want %q", cfg.Cluster.Host, "10.0.0.1")
	}
	if cfg.Cluster.GossipPort != 7100 {
		t.Errorf("Cluster.GossipPort = %d, want %d", cfg.Cluster.GossipPort, 7100)
	}
	if len(cfg.Cluster.Seeds) != 2 || cfg.Cluster.Seeds[0] != "10.0.0.2:7100" || cfg.Cluster.Seeds[1] != "10.0.0.3:7100" {
		t.Errorf("Cluster.Seeds = %v, want [10.0.0.2:7100 10.0.0.3:7100]", cfg.Cluster.Seeds)
	}
}

func TestApplyEnvOverrides_ClusterDefaults(t *testing.T) {
	// No cluster env vars set — cluster config stays at defaults
	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	if cfg.Cluster.Enabled {
		t.Error("Cluster.Enabled = true, want false")
	}
	if cfg.Cluster.GossipPort != 7000 {
		t.Errorf("Cluster.GossipPort = %d, want %d", cfg.Cluster.GossipPort, 7000)
	}
	if cfg.Cluster.Seeds != nil {
		t.Errorf("Cluster.Seeds = %v, want nil", cfg.Cluster.Seeds)
	}
}

func TestApplyEnvOverrides_InvalidGossipPort(t *testing.T) {
	os.Setenv("DMQTT_CLUSTER_GOSSIP_PORT", "notanumber")
	defer os.Unsetenv("DMQTT_CLUSTER_GOSSIP_PORT")

	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	// Invalid port should be ignored, keep default
	if cfg.Cluster.GossipPort != 7000 {
		t.Errorf("Cluster.GossipPort = %d, want %d (default)", cfg.Cluster.GossipPort, 7000)
	}
}

func TestApplyEnvOverrides_ReplicationVars(t *testing.T) {
	cfg := config.DefaultConfig()

	t.Setenv("DMQTT_REPLICATION_ENABLED", "true")
	t.Setenv("DMQTT_REPLICATION_REPLICA_COUNT", "3")
	t.Setenv("DMQTT_REPLICATION_SYNC_QOS2", "false")
	t.Setenv("DMQTT_REPLICATION_TAKEOVER_TIMEOUT_MS", "5000")
	applyEnvOverrides(cfg)

	if !cfg.Replication.Enabled {
		t.Error("Replication.Enabled should be true")
	}
	if cfg.Replication.ReplicaCount != 3 {
		t.Errorf("ReplicaCount = %d, want 3", cfg.Replication.ReplicaCount)
	}
	if cfg.Replication.SyncQoS2 {
		t.Error("SyncQoS2 should be false")
	}
	if cfg.Replication.TakeoverTimeoutMs != 5000 {
		t.Errorf("TakeoverTimeoutMs = %d, want 5000", cfg.Replication.TakeoverTimeoutMs)
	}
}

func TestApplyEnvOverrides_ReplicationDefaults(t *testing.T) {
	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	if cfg.Replication.Enabled {
		t.Error("default Replication.Enabled should be false")
	}
	if cfg.Replication.ReplicaCount != 2 {
		t.Errorf("default ReplicaCount = %d, want 2", cfg.Replication.ReplicaCount)
	}
}
