package bench

import (
	"testing"
	"time"
)

func TestMetrics_ConnCounts(t *testing.T) {
	m := NewMetrics()
	m.ConnSuccess.Add(10)
	m.ConnFail.Add(2)
	if m.ConnSuccess.Load() != 10 {
		t.Fatalf("expected 10 conn success, got %d", m.ConnSuccess.Load())
	}
	if m.ConnFail.Load() != 2 {
		t.Fatalf("expected 2 conn fail, got %d", m.ConnFail.Load())
	}
}

func TestMetrics_MsgCounts(t *testing.T) {
	m := NewMetrics()
	m.MsgSent.Add(100)
	m.MsgRecv.Add(95)
	m.MsgFail.Add(5)
	if m.MsgSent.Load() != 100 {
		t.Fatalf("expected 100 sent, got %d", m.MsgSent.Load())
	}
	if m.MsgRecv.Load() != 95 {
		t.Fatalf("expected 95 recv, got %d", m.MsgRecv.Load())
	}
}

func TestMetrics_RecordLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordConnLatency(1 * time.Millisecond)
	m.RecordConnLatency(5 * time.Millisecond)
	m.RecordConnLatency(10 * time.Millisecond)

	p := m.ConnLatencyPercentiles()
	if p.P50 < 1*time.Millisecond || p.P50 > 10*time.Millisecond {
		t.Fatalf("P50 out of range: %v", p.P50)
	}
	if p.P99 != 10*time.Millisecond {
		t.Fatalf("P99 expected 10ms, got %v", p.P99)
	}
	if p.Count != 3 {
		t.Fatalf("expected count 3, got %d", p.Count)
	}
}

func TestMetrics_Percentiles_Empty(t *testing.T) {
	m := NewMetrics()
	p := m.ConnLatencyPercentiles()
	if p.P50 != 0 || p.P99 != 0 || p.Count != 0 {
		t.Fatalf("empty percentiles should be zero, got %+v", p)
	}
}

func TestMetrics_PubLatency(t *testing.T) {
	m := NewMetrics()
	for i := 1; i <= 100; i++ {
		m.RecordPubLatency(time.Duration(i) * time.Millisecond)
	}
	p := m.PubLatencyPercentiles()
	if p.Count != 100 {
		t.Fatalf("expected 100 samples, got %d", p.Count)
	}
	// P50 should be around 50ms
	if p.P50 < 45*time.Millisecond || p.P50 > 55*time.Millisecond {
		t.Fatalf("P50 out of range: %v", p.P50)
	}
	// P99 should be around 99ms
	if p.P99 < 95*time.Millisecond || p.P99 > 100*time.Millisecond {
		t.Fatalf("P99 out of range: %v", p.P99)
	}
}

func TestMetrics_E2ELatency(t *testing.T) {
	m := NewMetrics()
	m.RecordE2ELatency(2 * time.Millisecond)
	m.RecordE2ELatency(4 * time.Millisecond)
	p := m.E2ELatencyPercentiles()
	if p.Count != 2 {
		t.Fatalf("expected 2 samples, got %d", p.Count)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()
	m.MsgSent.Add(50)
	m.MsgRecv.Add(45)
	m.ConnSuccess.Add(10)
	s := m.Snapshot()
	if s.Sent != 50 || s.Recv != 45 || s.ConnOK != 10 {
		t.Fatalf("snapshot mismatch: %+v", s)
	}
}
