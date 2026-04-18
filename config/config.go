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

	// Log level: "debug", "info", "warn", "error" (default: "info")
	LogLevel string

	// HTTP API address for metrics and health (default: ":9090")
	HTTPAddr string

	// Authentication credentials file (JSON). Empty = no auth (NoopAuth).
	AuthFile string

	// TLS listener address (e.g., ":8883"). Empty = TLS disabled.
	TLSAddr string

	// Path to TLS certificate file (PEM). Required if TLSAddr is set.
	TLSCertFile string

	// Path to TLS private key file (PEM). Required if TLSAddr is set.
	TLSKeyFile string

	// WebSocket listener address (e.g., ":8080"). Empty = disabled.
	WSAddr string

	// WebSocket over TLS listener address (e.g., ":8443"). Empty = disabled.
	// Requires TLSCertFile and TLSKeyFile to be set.
	WSSAddr string

	// Rate limiting configuration
	RateLimit RateLimitConfig

	// Circuit breaker configuration
	CircuitBreaker CircuitBreakerConfig

	// Offline message tiering
	OfflineTier OfflineTierConfig

	// Maximum inflight messages per client for flow control (default: 65535)
	MaxInflight uint16

	// Shard migration configuration
	Migration MigrationConfig

	// Audit interceptor configuration
	Audit AuditConfig

	// Rule engine configuration
	RuleEngine RuleEngineConfig

	// Tenant configuration
	Tenant TenantConfig
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

	// TransportPort is the port for inter-node message forwarding (default: GossipPort+1000)
	TransportPort int

	// Seeds is a list of existing node gossip addresses to join
	Seeds []string

	// VirtualNodes is the number of virtual nodes per physical node in the hash ring (default: 150)
	VirtualNodes int

	// ReplicaCount is the number of replicas per shard (default: 3)
	ReplicaCount int
}

// RateLimitConfig holds rate limiting settings.
type RateLimitConfig struct {
	Enabled        bool    `json:"enabled"`
	ClientMsgRate  float64 `json:"client_msg_rate"`
	ClientMsgBurst int     `json:"client_msg_burst"`
	ConnectRate    float64 `json:"connect_rate"`
	ConnectBurst   int     `json:"connect_burst"`
	GlobalMsgRate  float64 `json:"global_msg_rate"`
	GlobalMsgBurst int     `json:"global_msg_burst"`
	MaxMessageSize int     `json:"max_message_size"`

	BackpressureEnabled  bool `json:"backpressure_enabled"`
	BackpressureQueueMax int  `json:"backpressure_queue_max"`

	AdaptiveEnabled bool `json:"adaptive_enabled"`

	DetectorEnabled        bool    `json:"detector_enabled"`
	DetectorHighRate       float64 `json:"detector_high_rate"`
	DetectorScoreThreshold float64 `json:"detector_score_threshold"`
}

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	Enabled        bool    `json:"enabled"`
	ErrorThreshold float64 `json:"error_threshold"`
	WindowSizeMs   int     `json:"window_size_ms"`
	OpenDurationMs int     `json:"open_duration_ms"`
	HalfOpenMax    int     `json:"half_open_max"`
}

// OfflineTierConfig holds per-priority offline message settings.
type OfflineTierConfig struct {
	HighMax int           `json:"high_max"`
	HighTTL time.Duration `json:"high_ttl"`
	MidMax  int           `json:"mid_max"`
	MidTTL  time.Duration `json:"mid_ttl"`
	LowMax  int           `json:"low_max"`
	LowTTL  time.Duration `json:"low_ttl"`
}

// MigrationConfig holds shard migration settings.
type MigrationConfig struct {
	MaxParallel     int  `json:"max_parallel"`      // Max concurrent migrations (default: 3)
	BatchSize       int  `json:"batch_size"`        // Devices per batch (default: 1000)
	BatchIntervalMs int  `json:"batch_interval_ms"` // Milliseconds between batches (default: 1000)
	MaxRetries      int  `json:"max_retries"`       // Data transfer retries (default: 3)
	AutoRebalance   bool `json:"auto_rebalance"`    // Auto-migrate on ring changes (default: true)
}

// AuditConfig holds audit interceptor settings.
type AuditConfig struct {
	Enabled    bool   `json:"enabled"`
	BufferSize int    `json:"buffer_size"`
	BackupPath string `json:"backup_path"`
}

// RuleEngineConfig holds rule engine settings.
type RuleEngineConfig struct {
	Enabled        bool          `json:"enabled"`
	RulesFile      string        `json:"rules_file"`
	WorkerPoolSize int           `json:"worker_pool_size"`
	WebhookTimeout time.Duration `json:"webhook_timeout"`
}

// TenantConfig holds multi-tenancy configuration.
type TenantConfig struct {
	Enabled     bool   `json:"enabled"`
	TenantsFile string `json:"tenants_file"`
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
		LogLevel: "info",
		HTTPAddr: ":9090",
		RateLimit: RateLimitConfig{
			Enabled:                false,
			ClientMsgRate:          100,
			ClientMsgBurst:         200,
			ConnectRate:            1000,
			ConnectBurst:           2000,
			GlobalMsgRate:          10_000_000,
			GlobalMsgBurst:         20_000_000,
			MaxMessageSize:         256 * 1024,
			BackpressureEnabled:    false,
			BackpressureQueueMax:   100_000,
			AdaptiveEnabled:        false,
			DetectorEnabled:        false,
			DetectorHighRate:       10_000,
			DetectorScoreThreshold: 80,
		},
		CircuitBreaker: CircuitBreakerConfig{
			Enabled:        false,
			ErrorThreshold: 0.1,
			WindowSizeMs:   5000,
			OpenDurationMs: 30000,
			HalfOpenMax:    5,
		},
		OfflineTier: OfflineTierConfig{
			HighMax: 1000,
			HighTTL: 7 * 24 * time.Hour,
			MidMax:  500,
			MidTTL:  3 * 24 * time.Hour,
			LowMax:  100,
			LowTTL:  24 * time.Hour,
		},
		MaxInflight: 65535,
		Migration: MigrationConfig{
			MaxParallel:     3,
			BatchSize:       1000,
			BatchIntervalMs: 1000,
			MaxRetries:      3,
			AutoRebalance:   true,
		},
		Audit: AuditConfig{
			Enabled:    false,
			BufferSize: 4096,
		},
		RuleEngine: RuleEngineConfig{
			Enabled:        false,
			WorkerPoolSize: 64,
			WebhookTimeout: 5 * time.Second,
		},
		Tenant: TenantConfig{
			Enabled: false,
		},
	}
}
