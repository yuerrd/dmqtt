package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/langzp/dmqtt/config"
	"github.com/langzp/dmqtt/internal/broker"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/langzp/dmqtt/internal/storage"
)

func main() {
	cfg := config.DefaultConfig()

	var store storage.Store
	if cfg.DataDir != "" {
		var err error
		store, err = storage.NewPebbleStore(cfg.DataDir)
		if err != nil {
			log.Fatalf("Failed to open storage at %s: %v", cfg.DataDir, err)
		}
		defer store.Close()
		log.Printf("Storage opened at %s", cfg.DataDir)
	} else {
		log.Println("Running in-memory mode (no persistence)")
	}

	b := broker.New(cfg.TCPAddr, store)

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
			log.Fatalf("Failed to create cluster: %v", err)
		}
		b.SetCluster(c)
		log.Printf("Cluster mode: node %s, gossip on :%d", cfg.Cluster.NodeID, cfg.Cluster.GossipPort)

		defer func() {
			log.Println("Leaving cluster...")
			c.Stop()
		}()
	} else {
		log.Println("Running in standalone mode (no cluster)")
	}

	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	fmt.Printf("DMQTT listening on %s\n", b.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("DMQTT shutting down...")
	b.Stop()
	fmt.Println("DMQTT stopped")
}
