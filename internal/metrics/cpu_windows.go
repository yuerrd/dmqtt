//go:build windows

package metrics

// cpuTimeSec is not available on Windows.
func cpuTimeSec() (float64, bool) {
	return 0, false
}
