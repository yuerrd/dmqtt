package main

import (
	"crypto/tls"
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
	"github.com/langzp/dmqtt/internal/rule"
	"github.com/langzp/dmqtt/internal/storage"
	"github.com/langzp/dmqtt/internal/tenant"
	"github.com/langzp/dmqtt/internal/transport"
)

func main() {
	cfg := config.DefaultConfig()
	applyEnvOverrides(cfg)

	logging.Init(cfg.LogLevel)

	// Validate TLS config
	if cfg.TLSAddr != "" && (cfg.TLSCertFile == "" || cfg.TLSKeyFile == "") {
		slog.Error("TLSAddr requires TLSCertFile and TLSKeyFile")
		os.Exit(1)
	}

	if cfg.WSSAddr != "" && (cfg.TLSCertFile == "" || cfg.TLSKeyFile == "") {
		slog.Error("WSSAddr requires TLSCertFile and TLSKeyFile")
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

	// Determine tenant resolver
	var tenantResolver tenant.TenantResolver
	if cs, ok := authn.(*auth.CredentialStore); ok {
		tenantResolver = cs
	} else if noop, ok := authn.(*auth.NoopAuth); ok {
		tenantResolver = noop
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
	var tenantManager *tenant.TenantManager
	if cfg.Tenant.Enabled {
		tenantManager = tenant.NewManager()
		if cfg.Tenant.TenantsFile != "" {
			if err := tenantManager.LoadTenants(cfg.Tenant.TenantsFile); err != nil {
				slog.Error("failed to load tenants", "error", err)
				os.Exit(1)
			}
		}
		if tenantResolver != nil {
			tenantInterceptor := tenant.NewInterceptor(tenantManager, tenantResolver)
			chain.Register(tenantInterceptor, plugin.WithTimeout(50*time.Millisecond))
			slog.Info("tenant interceptor enabled", "tenants", tenantManager.TenantCount())
		} else {
			slog.Warn("tenant enabled but no resolver available")
		}
	}
	if cfg.Audit.Enabled {
		auditInterceptor := audit.New(audit.Config{
			BufferSize: cfg.Audit.BufferSize,
			BackupPath: cfg.Audit.BackupPath,
		}, nil)
		chain.Register(auditInterceptor, plugin.WithTimeout(50*time.Millisecond))
		slog.Info("audit interceptor enabled", "bufferSize", cfg.Audit.BufferSize)
	}
	var ruleEngine *rule.Engine
	if cfg.RuleEngine.Enabled {
		var err error
		ruleEngine, err = rule.NewEngine(b.RouteMessage, rule.EngineConfig{
			WorkerPoolSize: cfg.RuleEngine.WorkerPoolSize,
			WebhookTimeout: cfg.RuleEngine.WebhookTimeout,
		})
		if err != nil {
			slog.Error("failed to create rule engine", "error", err)
			os.Exit(1)
		}
		if cfg.RuleEngine.RulesFile != "" {
			if err := ruleEngine.LoadRules(cfg.RuleEngine.RulesFile); err != nil {
				slog.Error("failed to load rules", "error", err)
				os.Exit(1)
			}
		}
		ruleInterceptor := rule.NewRuleInterceptor(ruleEngine)
		chain.Register(ruleInterceptor)
		slog.Info("rule engine enabled", "rules_file", cfg.RuleEngine.RulesFile, "rules", ruleEngine.RuleCount())
	}
	// Load external plugins
	if len(cfg.Plugins) > 0 {
		loader := plugin.NewPluginLoader()
		extInterceptors, err := loader.LoadAll(cfg.Plugins)
		if err != nil {
			slog.Error("failed to load external plugin", "error", err)
			os.Exit(1)
		}
		for _, ei := range extInterceptors {
			chain.Register(ei)
			slog.Info("external plugin loaded", "name", ei.Name())
		}
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

		// Replication configuration
		if cfg.Replication.Enabled {
			replicator := cluster.NewReplicator(
				c.SelfID(),
				c.Transport(),
				c.Ring(),
				cfg.Replication.ReplicaCount,
			)
			c.SetReplicator(replicator)

			takeoverTimeout := time.Duration(cfg.Replication.TakeoverTimeoutMs) * time.Millisecond
			takeoverMgr := cluster.NewTakeoverManager(
				c.SelfID(),
				c.Transport(),
				b,
				&cluster.LWWResolver{},
				takeoverTimeout,
			)
			c.SetTakeoverManager(takeoverMgr)

			slog.Info("replication enabled",
				"replicas", cfg.Replication.ReplicaCount,
				"syncQoS2", cfg.Replication.SyncQoS2,
				"takeoverTimeout", takeoverTimeout,
			)
		}

		defer func() {
			slog.Info("leaving cluster")
			c.Stop()
		}()
	} else {
		slog.Info("running in standalone mode, no cluster")
	}

	httpSrv := httpapi.New(cfg.HTTPAddr, b)
	httpSrv.SetBrokerAPI(b)
	if cfg.Cluster.Enabled && b.Cluster() != nil {
		httpSrv.SetClusterAPI(b.Cluster())
		if b.Cluster().Coordinator() != nil {
			httpSrv.SetMigrationCoordinator(b.Cluster().Coordinator())
		}
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

	if cfg.WSAddr != "" {
		wsLn, err := transport.NewWSListener(cfg.WSAddr, nil)
		if err != nil {
			slog.Error("failed to create WebSocket listener", "addr", cfg.WSAddr, "error", err)
			os.Exit(1)
		}
		listeners = append(listeners, wsLn)
		slog.Info("WebSocket listener enabled", "addr", wsLn.Addr())
	}

	if cfg.WSSAddr != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			slog.Error("failed to load TLS cert for WSS", "error", err)
			os.Exit(1)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		wssLn, err := transport.NewWSListener(cfg.WSSAddr, tlsCfg)
		if err != nil {
			slog.Error("failed to create WSS listener", "addr", cfg.WSSAddr, "error", err)
			os.Exit(1)
		}
		listeners = append(listeners, wssLn)
		slog.Info("WSS listener enabled", "addr", wssLn.Addr())
	}

	// Start broker with all listeners
	go func() {
		if err := b.Serve(listeners...); err != nil {
			slog.Error("broker serve failed", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for sig := range sigCh {
		if sig == syscall.SIGHUP {
			if ruleEngine != nil && cfg.RuleEngine.RulesFile != "" {
				slog.Info("SIGHUP received, reloading rules", "file", cfg.RuleEngine.RulesFile)
				if err := ruleEngine.LoadRules(cfg.RuleEngine.RulesFile); err != nil {
					slog.Error("failed to reload rules", "error", err)
				} else {
					slog.Info("rules reloaded", "rules", ruleEngine.RuleCount())
				}
			}
			if tenantManager != nil && cfg.Tenant.TenantsFile != "" {
				slog.Info("SIGHUP received, reloading tenants", "file", cfg.Tenant.TenantsFile)
				if err := tenantManager.LoadTenants(cfg.Tenant.TenantsFile); err != nil {
					slog.Error("failed to reload tenants", "error", err)
				} else {
					slog.Info("tenants reloaded", "tenants", tenantManager.TenantCount())
				}
			}
			continue
		}
		break
	}

	fmt.Println("DMQTT shutting down...")
	if ruleEngine != nil {
		ruleEngine.Close()
	}
	b.Stop()
	fmt.Println("DMQTT stopped")
}
