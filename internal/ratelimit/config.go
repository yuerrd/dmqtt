package ratelimit

import "time"

// Config holds all rate limiting configuration.
type Config struct {
	Enabled  bool
	Global   GlobalConfig
	Client   ClientConfig
	Topic    TopicConfig
	Backpressure BackpressureConfig
	Adaptive     AdaptiveConfig
	Detector     DetectorConfig
	MaxMessageSize int // bytes, default 262144 (256KB)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Enabled: false,
		Global: GlobalConfig{
			IngressRate:  10_000_000,
			IngressBurst: 20_000_000,
			ConnectRate:  1000,
			ConnectBurst: 2000,
		},
		Client: ClientConfig{
			MsgRate:      100,
			MsgBurst:     200,
			BlacklistTTL: 60 * time.Second,
			CleanupInterval: 5 * time.Minute,
		},
		Topic: TopicConfig{
			DefaultRate:      0, // 0 = no limit
			DefaultBurst:     0,
			HotspotThreshold: 100_000,
			HotspotWindow:    10 * time.Second,
			TopicConfigs:     make(map[string]TopicLimitEntry),
		},
		Backpressure: BackpressureConfig{
			Enabled:           false,
			QueueSizeMax:      100_000,
			CriticalThreshold: 0.9,
			SevereThreshold:   0.7,
			ModerateThreshold: 0.5,
			CheckInterval:     time.Second,
		},
		Adaptive: AdaptiveConfig{
			Enabled:       false,
			CheckInterval: 5 * time.Second,
			HighLoad:      0.9,
			MediumLoad:    0.7,
			LowLoad:       0.3,
		},
		Detector: DetectorConfig{
			Enabled:            false,
			HighRateThreshold:  10_000,
			ScanThreshold:      100,
			ReconnectThreshold: 20,
			ScoreThreshold:     80,
			DecayInterval:      5 * time.Minute,
		},
		MaxMessageSize: 256 * 1024,
	}
}

type GlobalConfig struct {
	IngressRate  float64 // msg/s
	IngressBurst int
	ConnectRate  float64 // conn/s/node
	ConnectBurst int
}

type ClientConfig struct {
	MsgRate         float64       // msg/s per client
	MsgBurst        int
	BlacklistTTL    time.Duration
	CleanupInterval time.Duration
}

type TopicConfig struct {
	DefaultRate      float64 // 0 = no limit
	DefaultBurst     int
	HotspotThreshold float64 // msg/s to trigger hotspot
	HotspotWindow    time.Duration
	TopicConfigs     map[string]TopicLimitEntry
}

type TopicLimitEntry struct {
	MaxPublishRate float64
	MaxBurst       int
}

type BackpressureConfig struct {
	Enabled           bool
	QueueSizeMax      int
	CriticalThreshold float64
	SevereThreshold   float64
	ModerateThreshold float64
	CheckInterval     time.Duration
}

type AdaptiveConfig struct {
	Enabled       bool
	CheckInterval time.Duration
	HighLoad      float64
	MediumLoad    float64
	LowLoad       float64
}

type DetectorConfig struct {
	Enabled            bool
	HighRateThreshold  float64 // msg/s
	ScanThreshold      int     // distinct topics/min
	ReconnectThreshold int     // reconnects/min
	ScoreThreshold     float64 // blacklist threshold
	DecayInterval      time.Duration
}
