package bench

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestFormatInterval_Pub(t *testing.T) {
	line := FormatIntervalLine(5*time.Second, IntervalData{
		PubRate:  9850,
		RecvRate: 9720,
		PubP50:  1200 * time.Microsecond,
		PubP99:  5800 * time.Microsecond,
		Failures: 3,
	})
	if !strings.Contains(line, "5s") {
		t.Fatalf("expected timestamp, got: %s", line)
	}
	if !strings.Contains(line, "9850") {
		t.Fatalf("expected pub rate 9850, got: %s", line)
	}
	if !strings.Contains(line, "fail: 3") {
		t.Fatalf("expected fail count, got: %s", line)
	}
}

func TestFormatInterval_Conn(t *testing.T) {
	line := FormatIntervalLine(10*time.Second, IntervalData{
		ConnRate: 500,
		ConnP50:  2 * time.Millisecond,
		ConnP99:  15 * time.Millisecond,
		Failures: 0,
	})
	if !strings.Contains(line, "10s") {
		t.Fatalf("expected timestamp, got: %s", line)
	}
	if !strings.Contains(line, "500") {
		t.Fatalf("expected conn rate 500, got: %s", line)
	}
}

func TestFormatSummary(t *testing.T) {
	var buf bytes.Buffer
	WriteSummary(&buf, Summary{
		Duration:   60 * time.Second,
		Clients:    100,
		TotalSent:  600000,
		TotalRecv:  599850,
		TotalFail:  150,
		PubRate:    10000,
		RecvRate:   9997,
		PubP50:     1200 * time.Microsecond,
		PubP99:     5500 * time.Microsecond,
		PubP999:    12300 * time.Microsecond,
		ConnOK:     100,
		ConnFail:   0,
	})
	out := buf.String()
	if !strings.Contains(out, "Summary") {
		t.Fatalf("expected Summary header, got: %s", out)
	}
	if !strings.Contains(out, "600000") || !strings.Contains(out, "599850") {
		t.Fatalf("expected total counts, got: %s", out)
	}
	if !strings.Contains(out, "10000") {
		t.Fatalf("expected pub rate, got: %s", out)
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Microsecond, "0.50ms"},
		{1500 * time.Microsecond, "1.50ms"},
		{2 * time.Second, "2000.00ms"},
	}
	for _, tt := range tests {
		got := FormatDuration(tt.d)
		if got != tt.want {
			t.Errorf("FormatDuration(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}
