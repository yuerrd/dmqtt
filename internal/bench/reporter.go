package bench

import (
	"fmt"
	"io"
	"time"
)

// IntervalData holds data for one reporting interval.
type IntervalData struct {
	ConnRate int64
	ConnP50  time.Duration
	ConnP99  time.Duration
	PubRate  int64
	RecvRate int64
	PubP50   time.Duration
	PubP99   time.Duration
	Failures int64
}

// Summary holds final test results.
type Summary struct {
	Duration  time.Duration
	Clients   int
	TotalSent int64
	TotalRecv int64
	TotalFail int64
	PubRate   int64
	RecvRate  int64
	PubP50    time.Duration
	PubP99    time.Duration
	PubP999   time.Duration
	E2EP50    time.Duration
	E2EP99    time.Duration
	E2EP999   time.Duration
	ConnOK    int64
	ConnFail  int64
	ConnP50   time.Duration
	ConnP99   time.Duration
}

// FormatDuration formats a duration as milliseconds with 2 decimal places.
func FormatDuration(d time.Duration) string {
	ms := float64(d.Microseconds()) / 1000.0
	return fmt.Sprintf("%.2fms", ms)
}

// FormatIntervalLine formats a single interval report line.
func FormatIntervalLine(elapsed time.Duration, d IntervalData) string {
	secs := int(elapsed.Seconds())
	if d.ConnRate > 0 {
		return fmt.Sprintf("[%3ds] conn: %d/s | p50: %s p99: %s | fail: %d",
			secs, d.ConnRate, FormatDuration(d.ConnP50), FormatDuration(d.ConnP99), d.Failures)
	}
	return fmt.Sprintf("[%3ds] pub: %d msg/s | recv: %d msg/s | lat p50: %s p99: %s | fail: %d",
		secs, d.PubRate, d.RecvRate, FormatDuration(d.PubP50), FormatDuration(d.PubP99), d.Failures)
}

// WriteSummary writes the final summary report to w.
func WriteSummary(w io.Writer, s Summary) {
	fmt.Fprintln(w, "============ Summary ============")
	fmt.Fprintf(w, "Duration:       %.1fs\n", s.Duration.Seconds())
	fmt.Fprintf(w, "Clients:        %d\n", s.Clients)
	if s.ConnOK > 0 || s.ConnFail > 0 {
		total := s.ConnOK + s.ConnFail
		rate := float64(0)
		if total > 0 {
			rate = float64(s.ConnOK) / float64(total) * 100
		}
		fmt.Fprintf(w, "Conn Success:   %d / %d (%.2f%%)\n", s.ConnOK, total, rate)
		if s.ConnP50 > 0 {
			fmt.Fprintf(w, "Conn Lat P50:   %s\n", FormatDuration(s.ConnP50))
			fmt.Fprintf(w, "Conn Lat P99:   %s\n", FormatDuration(s.ConnP99))
		}
	}
	if s.TotalSent > 0 {
		fmt.Fprintf(w, "Total Sent:     %d\n", s.TotalSent)
		fmt.Fprintf(w, "Total Recv:     %d\n", s.TotalRecv)
		fmt.Fprintf(w, "Total Fail:     %d\n", s.TotalFail)
		fmt.Fprintf(w, "Pub Rate:       %d msg/s\n", s.PubRate)
		if s.RecvRate > 0 {
			fmt.Fprintf(w, "Recv Rate:      %d msg/s\n", s.RecvRate)
		}
		fmt.Fprintf(w, "Pub Lat P50:    %s\n", FormatDuration(s.PubP50))
		fmt.Fprintf(w, "Pub Lat P99:    %s\n", FormatDuration(s.PubP99))
		fmt.Fprintf(w, "Pub Lat P999:   %s\n", FormatDuration(s.PubP999))
	}
	if s.E2EP50 > 0 {
		fmt.Fprintf(w, "E2E Lat P50:    %s\n", FormatDuration(s.E2EP50))
		fmt.Fprintf(w, "E2E Lat P99:    %s\n", FormatDuration(s.E2EP99))
		fmt.Fprintf(w, "E2E Lat P999:   %s\n", FormatDuration(s.E2EP999))
	}
	fmt.Fprintln(w, "=================================")
}
