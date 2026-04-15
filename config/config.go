package config

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
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		TCPAddr:             ":1883",
		MaxConnections:      0,
		MaxPacketSize:       1 * 1024 * 1024,
		KeepAliveMultiplier: 1.5,
	}
}
