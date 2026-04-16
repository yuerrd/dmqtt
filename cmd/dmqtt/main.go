package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/langzp/dmqtt/config"
	"github.com/langzp/dmqtt/internal/auth"
	"github.com/langzp/dmqtt/internal/broker"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/langzp/dmqtt/internal/httpapi"
	"github.com/langzp/dmqtt/internal/logging"
	"github.com/langzp/dmqtt/internal/metrics"
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

		defer func() {
			slog.Info("leaving cluster")
			c.Stop()
		}()
	} else {
		slog.Info("running in standalone mode, no cluster")
	}

	httpSrv := httpapi.New(cfg.HTTPAddr, b)
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
