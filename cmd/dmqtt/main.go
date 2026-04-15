package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/langzp/dmqtt/config"
)

func main() {
	cfg := config.DefaultConfig()
	fmt.Printf("DMQTT starting on %s\n", cfg.TCPAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("DMQTT shutting down")
}
