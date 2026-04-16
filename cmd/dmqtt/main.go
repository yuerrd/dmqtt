package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/langzp/dmqtt/config"
	"github.com/langzp/dmqtt/internal/broker"
)

func main() {
	cfg := config.DefaultConfig()

	b := broker.New(cfg.TCPAddr, nil)
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
