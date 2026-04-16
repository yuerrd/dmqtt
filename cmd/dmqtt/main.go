package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/langzp/dmqtt/config"
	"github.com/langzp/dmqtt/internal/broker"
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
