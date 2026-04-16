package cluster

import "testing"

func TestNodeInfo_Addrs(t *testing.T) {
	n := NodeInfo{
		ID:         "node-1",
		Host:       "192.168.1.10",
		GossipPort: 7000,
		MQTTPort:   1883,
		Region:     "cn",
	}

	if got := n.GossipAddr(); got != "192.168.1.10:7000" {
		t.Errorf("GossipAddr() = %q, want %q", got, "192.168.1.10:7000")
	}
	if got := n.MQTTAddr(); got != "192.168.1.10:1883" {
		t.Errorf("MQTTAddr() = %q, want %q", got, "192.168.1.10:1883")
	}
}

func TestNodeInfo_MarshalRoundTrip(t *testing.T) {
	orig := NodeInfo{
		ID:         "node-42",
		Host:       "10.0.0.1",
		GossipPort: 7001,
		MQTTPort:   1884,
		Region:     "us-west",
	}

	data, err := orig.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := UnmarshalNodeInfo(data)
	if err != nil {
		t.Fatalf("UnmarshalNodeInfo: %v", err)
	}

	if got != orig {
		t.Errorf("roundtrip = %+v, want %+v", got, orig)
	}
}

func TestNodeInfo_MarshalOmitsEmptyRegion(t *testing.T) {
	n := NodeInfo{ID: "n1", Host: "localhost", GossipPort: 7000, MQTTPort: 1883}
	data, _ := n.Marshal()
	s := string(data)
	if contains(s, "region") {
		t.Errorf("expected region omitted, got %s", s)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
