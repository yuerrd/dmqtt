package bench

import (
	"testing"
	"time"
)

func TestExpandTopic(t *testing.T) {
	tests := []struct {
		tmpl string
		id   int
		want string
	}{
		{"bench/%i", 0, "bench/0"},
		{"bench/%i", 42, "bench/42"},
		{"sensor/%i/data", 7, "sensor/7/data"},
		{"fixed/topic", 5, "fixed/topic"},
	}
	for _, tt := range tests {
		got := ExpandTopic(tt.tmpl, tt.id)
		if got != tt.want {
			t.Errorf("ExpandTopic(%q, %d) = %q, want %q", tt.tmpl, tt.id, got, tt.want)
		}
	}
}

func TestRunConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     RunConfig
		wantErr bool
	}{
		{
			name: "valid pub",
			cfg: RunConfig{
				Broker:   "tcp://localhost:1883",
				Clients:  10,
				Duration: 10 * time.Second,
				Mode:     ModePub,
				Topic:    "bench/%i",
				QoS:      0,
				Rate:     1,
				Size:     256,
			},
			wantErr: false,
		},
		{
			name:    "missing broker",
			cfg:     RunConfig{Clients: 10, Duration: 10 * time.Second, Mode: ModePub},
			wantErr: true,
		},
		{
			name:    "zero clients",
			cfg:     RunConfig{Broker: "tcp://localhost:1883", Clients: 0, Duration: 10 * time.Second, Mode: ModePub},
			wantErr: true,
		},
		{
			name:    "invalid qos",
			cfg:     RunConfig{Broker: "tcp://localhost:1883", Clients: 1, Duration: 10 * time.Second, Mode: ModePub, QoS: 3},
			wantErr: true,
		},
		{
			name: "valid conn",
			cfg: RunConfig{
				Broker:   "tcp://localhost:1883",
				Clients:  100,
				Duration: 5 * time.Second,
				Mode:     ModeConn,
			},
			wantErr: false,
		},
		{
			name: "valid mixed",
			cfg: RunConfig{
				Broker:     "tcp://localhost:1883",
				Clients:    100,
				Duration:   10 * time.Second,
				Mode:       ModeMixed,
				Topic:      "bench/%i",
				PubClients: 50,
				SubClients: 50,
				Rate:       1,
				Size:       256,
			},
			wantErr: false,
		},
		{
			name: "mixed clients mismatch",
			cfg: RunConfig{
				Broker:     "tcp://localhost:1883",
				Clients:    100,
				Duration:   10 * time.Second,
				Mode:       ModeMixed,
				PubClients: 60,
				SubClients: 60,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGeneratePayload(t *testing.T) {
	p := GeneratePayload(256)
	if len(p) != 256 {
		t.Fatalf("expected 256 bytes, got %d", len(p))
	}
	// First 8 bytes are timestamp (nanoseconds as big-endian int64)
	if p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0 &&
		p[4] == 0 && p[5] == 0 && p[6] == 0 && p[7] == 0 {
		t.Fatal("timestamp should not be all zeros")
	}
}

func TestExtractTimestamp(t *testing.T) {
	p := GeneratePayload(256)
	ts := ExtractTimestamp(p)
	now := time.Now()
	diff := now.Sub(ts)
	if diff < 0 || diff > 1*time.Second {
		t.Fatalf("extracted timestamp too far from now: diff=%v", diff)
	}
}

func TestExtractTimestamp_TooShort(t *testing.T) {
	ts := ExtractTimestamp([]byte{1, 2, 3})
	if !ts.IsZero() {
		t.Fatal("expected zero time for short payload")
	}
}
