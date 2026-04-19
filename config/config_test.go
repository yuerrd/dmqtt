package config

import "testing"

func TestValidate_Valid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidate_EmptyTCPAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TCPAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty TCPAddr")
	}
}

func TestValidate_ClusterWithoutNodeID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.NodeID = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for cluster without NodeID")
	}
}

func TestValidate_ReplicationWithoutCluster(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Replication.Enabled = true
	cfg.Cluster.Enabled = false
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for replication without cluster")
	}
}

func TestValidate_InvalidKeepAlive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepAliveMultiplier = 0.5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for KeepAliveMultiplier < 1")
	}
}
