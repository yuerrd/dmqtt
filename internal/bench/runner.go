package bench

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Mode represents the benchmark mode.
type Mode string

const (
	ModeConn  Mode = "conn"
	ModePub   Mode = "pub"
	ModeSub   Mode = "sub"
	ModeMixed Mode = "mixed"
)

// RunConfig holds all configuration for a benchmark run.
type RunConfig struct {
	Broker     string
	Clients    int
	Duration   time.Duration
	Interval   time.Duration
	Mode       Mode
	Topic      string
	QoS        byte
	Rate       int
	Size       int
	Retain     bool
	Username   string
	Password   string
	PubClients int
	SubClients int
}

// Validate checks the configuration for errors.
func (c *RunConfig) Validate() error {
	if c.Broker == "" {
		return fmt.Errorf("broker address is required")
	}
	if c.Clients <= 0 {
		return fmt.Errorf("clients must be > 0")
	}
	if c.QoS > 2 {
		return fmt.Errorf("qos must be 0, 1, or 2")
	}
	if c.Mode == ModeMixed {
		if c.PubClients+c.SubClients > c.Clients {
			return fmt.Errorf("pub-clients (%d) + sub-clients (%d) exceeds total clients (%d)",
				c.PubClients, c.SubClients, c.Clients)
		}
	}
	return nil
}

// ExpandTopic replaces %i in the topic template with the client index.
func ExpandTopic(tmpl string, id int) string {
	return strings.ReplaceAll(tmpl, "%i", strconv.Itoa(id))
}

// GeneratePayload creates a payload of the given size with a nanosecond timestamp in the first 8 bytes.
func GeneratePayload(size int) []byte {
	if size < 8 {
		size = 8
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
	return buf
}

// ExtractTimestamp reads the nanosecond timestamp from the first 8 bytes of a payload.
func ExtractTimestamp(payload []byte) time.Time {
	if len(payload) < 8 {
		return time.Time{}
	}
	ns := binary.BigEndian.Uint64(payload[:8])
	return time.Unix(0, int64(ns))
}

// Runner manages the benchmark lifecycle.
type Runner struct {
	cfg     RunConfig
	metrics *Metrics
}

// NewRunner creates a new Runner.
func NewRunner(cfg RunConfig) *Runner {
	return &Runner{cfg: cfg, metrics: NewMetrics()}
}

// GetMetrics returns the runner's metrics.
func (r *Runner) GetMetrics() *Metrics { return r.metrics }

// Run executes the benchmark and blocks until done or context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.cfg.Validate(); err != nil {
		return err
	}

	switch r.cfg.Mode {
	case ModeConn:
		return r.runConn(ctx)
	case ModePub:
		return r.runPub(ctx)
	case ModeSub:
		return r.runSub(ctx)
	case ModeMixed:
		return r.runMixed(ctx)
	default:
		return fmt.Errorf("unknown mode: %s", r.cfg.Mode)
	}
}

func (r *Runner) newClient(id int) mqtt.Client {
	opts := mqtt.NewClientOptions().
		AddBroker(r.cfg.Broker).
		SetClientID(fmt.Sprintf("dmqtt-bench-%d", id)).
		SetAutoReconnect(false).
		SetCleanSession(true).
		SetConnectTimeout(10 * time.Second)
	if r.cfg.Username != "" {
		opts.SetUsername(r.cfg.Username)
	}
	if r.cfg.Password != "" {
		opts.SetPassword(r.cfg.Password)
	}
	return mqtt.NewClient(opts)
}

func (r *Runner) connectClient(c mqtt.Client) error {
	start := time.Now()
	token := c.Connect()
	token.Wait()
	elapsed := time.Since(start)
	if err := token.Error(); err != nil {
		r.metrics.ConnFail.Add(1)
		return err
	}
	r.metrics.ConnSuccess.Add(1)
	r.metrics.RecordConnLatency(elapsed)
	return nil
}

func (r *Runner) runConn(ctx context.Context) error {
	clients := make([]mqtt.Client, r.cfg.Clients)
	var wg sync.WaitGroup

	for i := 0; i < r.cfg.Clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := r.newClient(id)
			clients[id] = c
			_ = r.connectClient(c)
		}(i)
	}
	wg.Wait()

	// Hold connections for duration
	select {
	case <-ctx.Done():
	case <-time.After(r.cfg.Duration):
	}

	// Disconnect all
	for _, c := range clients {
		if c != nil && c.IsConnected() {
			c.Disconnect(250)
		}
	}
	return nil
}

func (r *Runner) runPub(ctx context.Context) error {
	var wg sync.WaitGroup
	deadline := time.After(r.cfg.Duration)

	for i := 0; i < r.cfg.Clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := r.newClient(id)
			if err := r.connectClient(c); err != nil {
				return
			}
			defer c.Disconnect(250)

			topic := ExpandTopic(r.cfg.Topic, id)
			interval := time.Second / time.Duration(r.cfg.Rate)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-deadline:
					return
				case <-ticker.C:
					payload := GeneratePayload(r.cfg.Size)
					start := time.Now()
					token := c.Publish(topic, r.cfg.QoS, r.cfg.Retain, payload)
					token.Wait()
					if err := token.Error(); err != nil {
						r.metrics.MsgFail.Add(1)
					} else {
						r.metrics.MsgSent.Add(1)
						r.metrics.RecordPubLatency(time.Since(start))
					}
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (r *Runner) runSub(ctx context.Context) error {
	var wg sync.WaitGroup
	deadline := time.After(r.cfg.Duration)

	for i := 0; i < r.cfg.Clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := r.newClient(id)
			if err := r.connectClient(c); err != nil {
				return
			}
			defer c.Disconnect(250)

			topic := ExpandTopic(r.cfg.Topic, id)
			token := c.Subscribe(topic, r.cfg.QoS, func(_ mqtt.Client, msg mqtt.Message) {
				r.metrics.MsgRecv.Add(1)
				ts := ExtractTimestamp(msg.Payload())
				if !ts.IsZero() {
					r.metrics.RecordE2ELatency(time.Since(ts))
				}
			})
			token.Wait()

			select {
			case <-ctx.Done():
			case <-deadline:
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (r *Runner) runMixed(ctx context.Context) error {
	var wg sync.WaitGroup
	deadline := time.After(r.cfg.Duration)

	// Start subscribers first
	for i := 0; i < r.cfg.SubClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := r.newClient(r.cfg.PubClients + id) // offset IDs
			if err := r.connectClient(c); err != nil {
				return
			}
			defer c.Disconnect(250)

			topic := ExpandTopic(r.cfg.Topic, id%r.cfg.PubClients)
			token := c.Subscribe(topic, r.cfg.QoS, func(_ mqtt.Client, msg mqtt.Message) {
				r.metrics.MsgRecv.Add(1)
				ts := ExtractTimestamp(msg.Payload())
				if !ts.IsZero() {
					r.metrics.RecordE2ELatency(time.Since(ts))
				}
			})
			token.Wait()

			select {
			case <-ctx.Done():
			case <-deadline:
			}
		}(i)
	}

	// Brief pause to let subscribers connect
	time.Sleep(500 * time.Millisecond)

	// Start publishers
	for i := 0; i < r.cfg.PubClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := r.newClient(id)
			if err := r.connectClient(c); err != nil {
				return
			}
			defer c.Disconnect(250)

			topic := ExpandTopic(r.cfg.Topic, id)
			interval := time.Second / time.Duration(r.cfg.Rate)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-deadline:
					return
				case <-ticker.C:
					payload := GeneratePayload(r.cfg.Size)
					start := time.Now()
					token := c.Publish(topic, r.cfg.QoS, r.cfg.Retain, payload)
					token.Wait()
					if err := token.Error(); err != nil {
						r.metrics.MsgFail.Add(1)
					} else {
						r.metrics.MsgSent.Add(1)
						r.metrics.RecordPubLatency(time.Since(start))
					}
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// StartReporter runs the periodic interval reporter in the background.
// It returns when ctx is done. Call this in a separate goroutine.
func (r *Runner) StartReporter(ctx context.Context) {
	if r.cfg.Interval <= 0 {
		r.cfg.Interval = 5 * time.Second
	}
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	start := time.Now()
	var lastSnap Snapshot

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start)
			snap := r.metrics.Snapshot()
			secs := r.cfg.Interval.Seconds()

			var data IntervalData
			if r.cfg.Mode == ModeConn {
				data.ConnRate = int64(float64(snap.ConnOK-lastSnap.ConnOK) / secs)
				p := r.metrics.ConnLatencyPercentiles()
				data.ConnP50 = p.P50
				data.ConnP99 = p.P99
				data.Failures = snap.ConnFL - lastSnap.ConnFL
			} else {
				data.PubRate = int64(float64(snap.Sent-lastSnap.Sent) / secs)
				data.RecvRate = int64(float64(snap.Recv-lastSnap.Recv) / secs)
				p := r.metrics.PubLatencyPercentiles()
				data.PubP50 = p.P50
				data.PubP99 = p.P99
				data.Failures = snap.Fail - lastSnap.Fail
			}

			fmt.Fprintln(os.Stdout, FormatIntervalLine(elapsed, data))
			lastSnap = snap
		}
	}
}

// BuildSummary creates a Summary from the current metrics state.
func (r *Runner) BuildSummary() Summary {
	snap := r.metrics.Snapshot()
	pubP := r.metrics.PubLatencyPercentiles()
	e2eP := r.metrics.E2ELatencyPercentiles()
	connP := r.metrics.ConnLatencyPercentiles()
	secs := r.cfg.Duration.Seconds()

	return Summary{
		Duration:  r.cfg.Duration,
		Clients:   r.cfg.Clients,
		TotalSent: snap.Sent,
		TotalRecv: snap.Recv,
		TotalFail: snap.Fail,
		PubRate:   int64(float64(snap.Sent) / secs),
		RecvRate:  int64(float64(snap.Recv) / secs),
		PubP50:    pubP.P50,
		PubP99:    pubP.P99,
		PubP999:   pubP.P999,
		E2EP50:    e2eP.P50,
		E2EP99:    e2eP.P99,
		E2EP999:   e2eP.P999,
		ConnOK:    snap.ConnOK,
		ConnFail:  snap.ConnFL,
		ConnP50:   connP.P50,
		ConnP99:   connP.P99,
	}
}
