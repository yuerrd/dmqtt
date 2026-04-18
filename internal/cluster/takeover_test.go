package cluster

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type mockTakeoverTransport struct {
	mu   sync.Mutex
	sent map[string][]interface{}
}

func newMockTakeoverTransport() *mockTakeoverTransport {
	return &mockTakeoverTransport{
		sent: make(map[string][]interface{}),
	}
}

func (m *mockTakeoverTransport) SendRaw(nodeID string, msg interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent[nodeID] = append(m.sent[nodeID], msg)
	return nil
}

type mockDisconnector struct {
	mu           sync.Mutex
	disconnected []string
}

func (d *mockDisconnector) DisconnectDevice(clientID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.disconnected = append(d.disconnected, clientID)
}

func TestTakeoverManager_RequestTakeover_Success(t *testing.T) {
	transport := newMockTakeoverTransport()
	disconnector := &mockDisconnector{}
	resolver := &LWWResolver{}

	tm := NewTakeoverManager("node-B", transport, disconnector, resolver, 2*time.Second)

	// Simulate: the remote node will respond with ACK
	go func() {
		time.Sleep(10 * time.Millisecond)
		resp := TakeoverResponse{
			Type:     MsgSessionTakeoverAck,
			ClientID: "c1",
			Success:  true,
		}
		respData, _ := json.Marshal(resp)
		tm.HandleTakeoverResponse(respData)
	}()

	result := tm.RequestTakeover("c1", "node-A", 5, time.Now().UnixNano())

	if !result {
		t.Error("RequestTakeover should succeed when ACK is received")
	}
}

func TestTakeoverManager_HandleIncoming_Accept(t *testing.T) {
	transport := newMockTakeoverTransport()
	disconnector := &mockDisconnector{}
	resolver := &LWWResolver{}

	tm := NewTakeoverManager("node-A", transport, disconnector, resolver, 2*time.Second)

	tm.SetLocalSession("c1", &SessionMeta{
		ClientID:         "c1",
		Epoch:            3,
		ConnectTimestamp: time.Now().Add(-time.Minute).UnixNano(),
		NodeID:           "node-A",
	})

	req := TakeoverRequest{
		Type:             MsgSessionTakeover,
		ClientID:         "c1",
		RequestNodeID:    "node-B",
		ConnectTimestamp: time.Now().UnixNano(),
		Epoch:            5,
	}

	tm.HandleTakeoverRequest(req)

	disconnector.mu.Lock()
	if len(disconnector.disconnected) != 1 || disconnector.disconnected[0] != "c1" {
		t.Errorf("disconnected = %v, want [c1]", disconnector.disconnected)
	}
	disconnector.mu.Unlock()

	transport.mu.Lock()
	if len(transport.sent["node-B"]) != 1 {
		t.Errorf("sent to node-B = %d, want 1", len(transport.sent["node-B"]))
	}
	transport.mu.Unlock()
}

func TestTakeoverManager_HandleIncoming_Reject(t *testing.T) {
	transport := newMockTakeoverTransport()
	disconnector := &mockDisconnector{}
	resolver := &LWWResolver{}

	tm := NewTakeoverManager("node-A", transport, disconnector, resolver, 2*time.Second)

	tm.SetLocalSession("c1", &SessionMeta{
		ClientID:         "c1",
		Epoch:            10,
		ConnectTimestamp: time.Now().UnixNano(),
		NodeID:           "node-A",
	})

	req := TakeoverRequest{
		Type:             MsgSessionTakeover,
		ClientID:         "c1",
		RequestNodeID:    "node-B",
		ConnectTimestamp: time.Now().Add(-time.Minute).UnixNano(),
		Epoch:            5,
	}

	tm.HandleTakeoverRequest(req)

	disconnector.mu.Lock()
	if len(disconnector.disconnected) != 0 {
		t.Errorf("should not disconnect, got %v", disconnector.disconnected)
	}
	disconnector.mu.Unlock()

	transport.mu.Lock()
	if len(transport.sent["node-B"]) != 1 {
		t.Fatalf("sent to node-B = %d, want 1", len(transport.sent["node-B"]))
	}
	respData, _ := json.Marshal(transport.sent["node-B"][0])
	var resp TakeoverResponse
	json.Unmarshal(respData, &resp)
	if resp.Success {
		t.Error("should be NACK")
	}
	transport.mu.Unlock()
}
