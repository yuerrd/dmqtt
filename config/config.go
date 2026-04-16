package config

import "time"

// Config holds the broker configuration.
type Config struct {
	// TCP listener address (e.g., ":1883")
	TCPAddr string

	// Maximum number of concurrent connections (0 = unlimited)
	MaxConnections int

	// Maximum incoming MQTT packet size in bytes (default: 1MB)
	MaxPacketSize int

	// Keep-alive timeout multiplier (default: 1.5x of client's keep-alive)
	KeepAliveMultiplier float64

	// Data directory for Pebble storage (empty = in-memory only)
	DataDir string

	// Maximum inflight messages per client (default: 20)
	InflightLimit int

	// Maximum offline messages per client (default: 1000)
	OfflineMaxPerClient int

	// Default TTL for offline messages (default: 24h)
	OfflineTTL time.Duration

	// Cluster configuration
	Cluster ClusterConfig
}

// ClusterConfig holds cluster-related settings.
type ClusterConfig struct {
	// Enabled activates cluster mode (default: false = standalone)
	Enabled bool

	// Name is the cluster name (default: "dmqtt")
	Name string

	// NodeID is the unique identifier for this node (required if Enabled)
	NodeID string

	// Host is the IP address for gossip and MQTT (default: "127.0.0.1")
	Host string

	// GossipPort is the port for memberlist communication (default: 7000)
	GossipPort int

	// Seeds is a list of existing node gossip addresses to join
	Seeds []string

	// VirtualNodes is the number of virtual nodes per physical node in the hash ring (default: 150)
	VirtualNodes int

	// ReplicaCount is the number of replicas per shard (default: 3)
	ReplicaCount int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		TCPAddr:             ":1883",
		MaxConnections:      0,
		MaxPacketSize:       1 * 1024 * 1024,
		KeepAliveMultiplier: 1.5,
		DataDir:             "",
		InflightLimit:       20,
		OfflineMaxPerClient: 1000,
		OfflineTTL:          24 * time.Hour,
		Cluster: ClusterConfig{
			Enabled:      false,
			Name:         "dmqtt",
			Host:         "127.0.0.1",
			GossipPort:   7000,
			VirtualNodes: 150,
			ReplicaCount: 3,
		},
	}
}
