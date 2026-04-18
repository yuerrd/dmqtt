package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/langzp/dmqtt/internal/bench"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcmd := os.Args[1]
	var mode bench.Mode
	switch subcmd {
	case "conn":
		mode = bench.ModeConn
	case "pub":
		mode = bench.ModePub
	case "sub":
		mode = bench.ModeSub
	case "mixed":
		mode = bench.ModeMixed
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", subcmd)
		printUsage()
		os.Exit(1)
	}

	fs := flag.NewFlagSet(subcmd, flag.ExitOnError)

	broker := fs.String("broker", "tcp://localhost:1883", "MQTT broker address")
	clients := fs.Int("clients", 100, "Number of concurrent clients")
	duration := fs.Duration("duration", 60*time.Second, "Test duration")
	interval := fs.Duration("interval", 5*time.Second, "Report interval")
	qos := fs.Int("qos", 0, "MQTT QoS level (0/1/2)")
	topic := fs.String("topic", "bench/%i", "Topic template (%i = client index)")
	username := fs.String("username", "", "MQTT username")
	password := fs.String("password", "", "MQTT password")
	rate := fs.Int("rate", 1, "Messages per second per client (pub/mixed)")
	size := fs.Int("size", 256, "Payload size in bytes (pub/mixed)")
	retain := fs.Bool("retain", false, "Set retain flag (pub/mixed)")
	pubClients := fs.Int("pub-clients", 50, "Publisher count (mixed mode)")
	subClients := fs.Int("sub-clients", 50, "Subscriber count (mixed mode)")

	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	cfg := bench.RunConfig{
		Broker:     *broker,
		Clients:    *clients,
		Duration:   *duration,
		Interval:   *interval,
		Mode:       mode,
		Topic:      *topic,
		QoS:        byte(*qos),
		Rate:       *rate,
		Size:       *size,
		Retain:     *retain,
		Username:   *username,
		Password:   *password,
		PubClients: *pubClients,
		SubClients: *subClients,
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("dmqtt-bench: mode=%s broker=%s clients=%d duration=%s\n",
		mode, *broker, *clients, *duration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	runner := bench.NewRunner(cfg)

	// Start periodic reporter
	go runner.StartReporter(ctx)

	// Run benchmark
	if err := runner.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Print summary
	fmt.Println()
	summary := runner.BuildSummary()
	bench.WriteSummary(os.Stdout, summary)
}

func printUsage() {
	fmt.Println(`dmqtt-bench - MQTT Broker Stress Testing Tool

Usage:
  dmqtt-bench <command> [flags]

Commands:
  conn    Connection benchmark (connect N clients, hold, disconnect)
  pub     Publish benchmark (N clients publish at rate R)
  sub     Subscribe benchmark (N clients subscribe and receive)
  mixed   Mixed mode (pub + sub clients simultaneously)
  help    Show this help

Flags (use "dmqtt-bench <command> -h" for details):
  --broker     Broker address (default: tcp://localhost:1883)
  --clients    Concurrent clients (default: 100)
  --duration   Test duration (default: 60s)
  --interval   Report interval (default: 5s)
  --qos        QoS level 0/1/2 (default: 0)
  --topic      Topic template, %i = client index (default: bench/%i)
  --rate       Messages/sec/client (default: 1)
  --size       Payload bytes (default: 256)

Examples:
  dmqtt-bench conn --clients 10000 --duration 30s
  dmqtt-bench pub --clients 100 --rate 10 --size 512 --duration 60s
  dmqtt-bench sub --clients 100 --topic "bench/#" --duration 60s
  dmqtt-bench mixed --pub-clients 50 --sub-clients 50 --rate 5`)
}
