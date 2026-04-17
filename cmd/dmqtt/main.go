package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/langzp/dmqtt/config"
	"github.com/langzp/dmqtt/internal/auth"
	"github.com/langzp/dmqtt/internal/broker"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/langzp/dmqtt/internal/httpapi"
	"github.com/langzp/dmqtt/internal/logging"
	"github.com/langzp/dmqtt/internal/metrics"
	"github.com/langzp/dmqtt/internal/plugin"
	"github.com/langzp/dmqtt/internal/plugin/audit"
	"github.com/langzp/dmqtt/internal/ratelimit"
	"github.com/langzp/dmqtt/internal/storage"
	"github.com/langzp/dmqtt/internal/transport"
)

func main() {
	cfg := config.DefaultConfig()

	logging.Init(cfg.LogLevel)

	// Validate TLS config
	if cfg.TLSAddr != "" && (cfg.TLSCertFile == "" || cfg.TLSKeyFile == "") {
		slog.Error("TLSAddr requires TLSCertFile and TLSKeyFile")
		os.Exit(1)
	}

	// Load authentication
	var authn auth.Authenticator
	var authz auth.Authorizer
	if cfg.AuthFile != "" {
		creds, err := auth.LoadCredentials(cfg.AuthFile)
		if err != nil {
			slog.Error("failed to load auth file", "path", cfg.AuthFile, "error", err)
			os.Exit(1)
		}
		authn = creds
		authz = creds
		slog.Info("authentication enabled", "file", cfg.AuthFile)
	} else {
		noop := &auth.NoopAuth{}
		authn = noop
		authz = noop
		slog.Info("running without authentication")
	}

	var store storage.Store
	if cfg.DataDir != "" {
		var err error
		store, err = storage.NewPebbleStore(cfg.DataDir)
		if err != nil {
			slog.Error("failed to open storage", "path", cfg.DataDir, "error", err)
			os.Exit(1)
		}
		defer store.Close()
		slog.Info("storage opened", "path", cfg.DataDir)
	} else {
		slog.Info("running in-memory mode, no persistence")
	}

	b := broker.New(cfg.TCPAddr, store)
	b.SetAuth(authn, authz)

	// Interceptor chain
	chain := plugin.NewInterceptorChain()
	if cfg.Audit.Enabled {
		auditInterceptor := audit.New(audit.Config{
			BufferSize: cfg.Audit.BufferSize,
			BackupPath: cfg.Audit.BackupPath,
		}, nil)
		chain.Register(auditInterceptor, plugin.WithTimeout(50*time.Millisecond))
		slog.Info("audit interceptor enabled", "bufferSize", cfg.Audit.BufferSize)
	}
	if err := chain.InitAll(); err != nil {
		slog.Error("failed to initialize interceptors", "error", err)
		os.Exit(1)
	}
	b.SetInterceptors(chain)
	defer chain.CloseAll()

	// Rate limiting
	if cfg.RateLimit.Enabled {
		rlCfg := &ratelimit.Config{
			Enabled: true,
			Global: ratelimit.GlobalConfig{
				IngressRate:  cfg.RateLimit.GlobalMsgRate,
				IngressBurst: cfg.RateLimit.GlobalMsgBurst,
				ConnectRate:  cfg.RateLimit.ConnectRate,
				ConnectBurst: cfg.RateLimit.ConnectBurst,
			},
			Client: ratelimit.ClientConfig{
				MsgRate:         cfg.RateLimit.ClientMsgRate,
				MsgBurst:        cfg.RateLimit.ClientMsgBurst,
				BlacklistTTL:    60 * time.Second,
				CleanupInterval: 5 * time.Minute,
			},
			Topic: ratelimit.TopicConfig{
				TopicConfigs: make(map[string]ratelimit.TopicLimitEntry),
			},
			Backpressure: ratelimit.BackpressureConfig{
				Enabled:           cfg.RateLimit.BackpressureEnabled,
				QueueSizeMax:      cfg.RateLimit.BackpressureQueueMax,
				CriticalThreshold: 0.9,
				SevereThreshold:   0.7,
				ModerateThreshold: 0.5,
				CheckInterval:     time.Second,
			},
			Adaptive: ratelimit.AdaptiveConfig{
				Enabled:       cfg.RateLimit.AdaptiveEnabled,
				CheckInterval: 5 * time.Second,
				HighLoad:      0.9,
				MediumLoad:    0.7,
				LowLoad:       0.3,
			},
			Detector: ratelimit.DetectorConfig{
				Enabled:            cfg.RateLimit.DetectorEnabled,
				HighRateThreshold:  cfg.RateLimit.DetectorHighRate,
				ScoreThreshold:     cfg.RateLimit.DetectorScoreThreshold,
				ScanThreshold:      100,
				ReconnectThreshold: 20,
				DecayInterval:      5 * time.Minute,
			},
			MaxMessageSize: cfg.RateLimit.MaxMessageSize,
		}
		b.SetRateLimiter(ratelimit.NewAggregateRateLimiter(rlCfg))
		slog.Info("rate limiting enabled",
			"clientRate", cfg.RateLimit.ClientMsgRate,
			"connectRate", cfg.RateLimit.ConnectRate,
		)
	}

	if cfg.Cluster.Enabled {
		clusterCfg := cluster.ClusterConfig{
			Enabled:       true,
			Name:          cfg.Cluster.Name,
			NodeID:        cfg.Cluster.NodeID,
			Host:          cfg.Cluster.Host,
			GossipPort:    cfg.Cluster.GossipPort,
			TransportPort: cfg.Cluster.TransportPort,
			MQTTPort:      1883,
			Seeds:         cfg.Cluster.Seeds,
			VirtualNodes:  cfg.Cluster.VirtualNodes,
			ReplicaCount:  cfg.Cluster.ReplicaCount,
		}

		c, err := cluster.NewCluster(clusterCfg)
		if err != nil {
			slog.Error("failed to create cluster", "error", err)
			os.Exit(1)
		}
		b.SetCluster(c)
		slog.Info("cluster mode enabled", "node", cfg.Cluster.NodeID, "gossipPort", cfg.Cluster.GossipPort)

		// Migration configuration
		migCfg := cluster.MigrationConfig{
			BatchSize:     cfg.Migration.BatchSize,
			BatchInterval: time.Duration(cfg.Migration.BatchIntervalMs) * time.Millisecond,
			MaxRetries:    cfg.Migration.MaxRetries,
		}
		c.SetMigrationBrokerAPI(b)
		c.SetMigrationConfig(migCfg)
		c.SetAutoRebalance(cfg.Migration.AutoRebalance)
		slog.Info("migration configured",
			"autoRebalance", cfg.Migration.AutoRebalance,
			"maxParallel", cfg.Migration.MaxParallel,
			"batchSize", cfg.Migration.BatchSize,
		)

		defer func() {
			slog.Info("leaving cluster")
			c.Stop()
		}()
	} else {
		slog.Info("running in standalone mode, no cluster")
	}

	httpSrv := httpapi.New(cfg.HTTPAddr, b)
	if cfg.Cluster.Enabled && b.Cluster() != nil && b.Cluster().Coordinator() != nil {
		httpSrv.SetMigrationCoordinator(b.Cluster().Coordinator())
	}
	if err := httpSrv.Start(); err != nil {
		slog.Error("failed to start HTTP API", "addr", cfg.HTTPAddr, "error", err)
		os.Exit(1)
	}
	slog.Info("HTTP API listening", "addr", httpSrv.Addr())
	defer httpSrv.Stop()

	done := make(chan struct{})
	metrics.StartCollector(b, done)
	defer close(done)

	// Build listener list
	var listeners []transport.Listener

	tcpLn, err := transport.NewTCPListener(cfg.TCPAddr)
	if err != nil {
		slog.Error("failed to create TCP listener", "addr", cfg.TCPAddr, "error", err)
		os.Exit(1)
	}
	listeners = append(listeners, tcpLn)

	if cfg.TLSAddr != "" {
		tlsLn, err := transport.NewTLSListener(cfg.TLSAddr, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			slog.Error("failed to create TLS listener", "addr", cfg.TLSAddr, "error", err)
			os.Exit(1)
		}
		listeners = append(listeners, tlsLn)
		slog.Info("TLS listener enabled", "addr", tlsLn.Addr())
	}

	// Start broker with all listeners
	go func() {
		if err := b.Serve(listeners...); err != nil {
			slog.Error("broker serve failed", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("DMQTT shutting down...")
	b.Stop()
	fmt.Println("DMQTT stopped")
}
