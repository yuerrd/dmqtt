package bench

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Percentiles holds calculated latency percentiles.
type Percentiles struct {
	P50   time.Duration
	P99   time.Duration
	P999  time.Duration
	Min   time.Duration
	Max   time.Duration
	Count int
}

// Snapshot holds a point-in-time copy of counter values.
type Snapshot struct {
	Sent   int64
	Recv   int64
	Fail   int64
	ConnOK int64
	ConnFL int64
}

// Metrics collects stress test statistics using atomic counters and mutex-protected latency slices.
type Metrics struct {
	ConnSuccess atomic.Int64
	ConnFail    atomic.Int64
	MsgSent     atomic.Int64
	MsgRecv     atomic.Int64
	MsgFail     atomic.Int64

	mu      sync.Mutex
	connLat []time.Duration
	pubLat  []time.Duration
	e2eLat  []time.Duration
}

// NewMetrics creates a new Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// RecordConnLatency records a connection establishment latency sample.
func (m *Metrics) RecordConnLatency(d time.Duration) {
	m.mu.Lock()
	m.connLat = append(m.connLat, d)
	m.mu.Unlock()
}

// RecordPubLatency records a publish acknowledgment latency sample.
func (m *Metrics) RecordPubLatency(d time.Duration) {
	m.mu.Lock()
	m.pubLat = append(m.pubLat, d)
	m.mu.Unlock()
}

// RecordE2ELatency records an end-to-end (publish to subscribe receive) latency sample.
func (m *Metrics) RecordE2ELatency(d time.Duration) {
	m.mu.Lock()
	m.e2eLat = append(m.e2eLat, d)
	m.mu.Unlock()
}

// ConnLatencyPercentiles returns percentiles for connection latency.
func (m *Metrics) ConnLatencyPercentiles() Percentiles {
	m.mu.Lock()
	cp := make([]time.Duration, len(m.connLat))
	copy(cp, m.connLat)
	m.mu.Unlock()
	return calcPercentiles(cp)
}

// PubLatencyPercentiles returns percentiles for publish latency.
func (m *Metrics) PubLatencyPercentiles() Percentiles {
	m.mu.Lock()
	cp := make([]time.Duration, len(m.pubLat))
	copy(cp, m.pubLat)
	m.mu.Unlock()
	return calcPercentiles(cp)
}

// E2ELatencyPercentiles returns percentiles for end-to-end latency.
func (m *Metrics) E2ELatencyPercentiles() Percentiles {
	m.mu.Lock()
	cp := make([]time.Duration, len(m.e2eLat))
	copy(cp, m.e2eLat)
	m.mu.Unlock()
	return calcPercentiles(cp)
}

// Snapshot returns a point-in-time copy of all counters.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Sent:   m.MsgSent.Load(),
		Recv:   m.MsgRecv.Load(),
		Fail:   m.MsgFail.Load(),
		ConnOK: m.ConnSuccess.Load(),
		ConnFL: m.ConnFail.Load(),
	}
}

func calcPercentiles(data []time.Duration) Percentiles {
	n := len(data)
	if n == 0 {
		return Percentiles{}
	}
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
	return Percentiles{
		P50:   data[percentileIndex(n, 0.50)],
		P99:   data[percentileIndex(n, 0.99)],
		P999:  data[percentileIndex(n, 0.999)],
		Min:   data[0],
		Max:   data[n-1],
		Count: n,
	}
}

func percentileIndex(n int, p float64) int {
	idx := int(math.Ceil(float64(n)*p)) - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}
