package cluster

import (
	"encoding/json"
	"sync"
	"testing"
)

type mockTransport struct {
	mu   sync.Mutex
	sent map[string][]interface{}
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		sent: make(map[string][]interface{}),
	}
}

func (m *mockTransport) SendRaw(nodeID string, msg interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent[nodeID] = append(m.sent[nodeID], msg)
	return nil
}

func (m *mockTransport) sentCount(nodeID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent[nodeID])
}

type mockRing struct {
	replicas []NodeInfo
}

func (r *mockRing) LocateDeviceN(deviceID string, n int) []NodeInfo {
	if n > len(r.replicas) {
		n = len(r.replicas)
	}
	return r.replicas[:n]
}

func TestReplicator_ReplicateOffline(t *testing.T) {
	transport := newMockTransport()
	ring := &mockRing{
		replicas: []NodeInfo{
			{ID: "node-1"},
			{ID: "node-2"},
			{ID: "node-3"},
		},
	}

	r := NewReplicator("node-1", transport, ring, 3)

	entries := []ReplicateOfflineEntry{
		{Topic: "t/1", Payload: []byte("p1"), QoS: 1},
	}

	r.ReplicateOffline("client-a", entries)
	r.Stop()
	if transport.sentCount("node-2") != 1 {
		t.Errorf("node-2 received %d, want 1", transport.sentCount("node-2"))
	}
	if transport.sentCount("node-3") != 1 {
		t.Errorf("node-3 received %d, want 1", transport.sentCount("node-3"))
	}
	if transport.sentCount("node-1") != 0 {
		t.Errorf("node-1 (self) received %d, want 0", transport.sentCount("node-1"))
	}
}

func TestReplicator_ReplicateWill(t *testing.T) {
	transport := newMockTransport()
	ring := &mockRing{
		replicas: []NodeInfo{
			{ID: "node-1"},
			{ID: "node-2"},
		},
	}

	r := NewReplicator("node-1", transport, ring, 3)
	r.ReplicateWill("client-b", "will/topic", []byte("will-payload"), 1, false)
	r.Stop()
	if transport.sentCount("node-2") != 1 {
		t.Errorf("node-2 received %d, want 1", transport.sentCount("node-2"))
	}
}

func TestReplicator_DeleteWill(t *testing.T) {
	transport := newMockTransport()
	ring := &mockRing{
		replicas: []NodeInfo{
			{ID: "node-1"},
			{ID: "node-2"},
			{ID: "node-3"},
		},
	}

	r := NewReplicator("node-1", transport, ring, 3)
	r.DeleteWill("client-c")
	r.Stop()
	if transport.sentCount("node-2") != 1 {
		t.Errorf("node-2 received %d, want 1", transport.sentCount("node-2"))
	}
	if transport.sentCount("node-3") != 1 {
		t.Errorf("node-3 received %d, want 1", transport.sentCount("node-3"))
	}
}

func TestReplicator_ClearOfflineReplica(t *testing.T) {
	transport := newMockTransport()
	ring := &mockRing{
		replicas: []NodeInfo{
			{ID: "node-1"},
			{ID: "node-2"},
		},
	}

	r := NewReplicator("node-1", transport, ring, 3)
	r.ClearOfflineReplica("client-d")
	r.Stop()
	if transport.sentCount("node-2") != 1 {
		t.Errorf("node-2 received %d, want 1", transport.sentCount("node-2"))
	}

	transport.mu.Lock()
	raw := transport.sent["node-2"][0]
	transport.mu.Unlock()
	data, _ := json.Marshal(raw)
	var ack ReplicateOfflineAckMessage
	json.Unmarshal(data, &ack)
	if ack.Type != MsgReplicateOfflineAck {
		t.Errorf("Type = %s, want %s", ack.Type, MsgReplicateOfflineAck)
	}
}
