package cluster

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
)

func TestReplicateOfflineMessage_WireRoundTrip(t *testing.T) {
	// Use net.Pipe to get a synchronous in-memory connection pair.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	msg := ReplicateOfflineMessage{
		Type:     MsgReplicateOffline,
		ClientID: "device-42",
		Messages: []ReplicateOfflineEntry{
			{
				Topic:     "sensor/temp",
				Payload:   []byte("23.5"),
				QoS:       1,
				Priority:  5,
				CreatedAt: 1000,
				ExpiresAt: 2000,
			},
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Write from one side, read from the other.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := writeFrame(client, data); err != nil {
			t.Errorf("writeFrame: %v", err)
		}
	}()

	received, err := readFrame(server)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	wg.Wait()

	var got ReplicateOfflineMessage
	if err := json.Unmarshal(received, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Type != MsgReplicateOffline {
		t.Errorf("Type = %q, want %q", got.Type, MsgReplicateOffline)
	}
	if got.ClientID != "device-42" {
		t.Errorf("ClientID = %q, want %q", got.ClientID, "device-42")
	}
	if len(got.Messages) != 1 {
		t.Fatalf("Messages len = %d, want 1", len(got.Messages))
	}
	entry := got.Messages[0]
	if entry.Topic != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", entry.Topic, "sensor/temp")
	}
	if string(entry.Payload) != "23.5" {
		t.Errorf("Payload = %q, want %q", entry.Payload, "23.5")
	}
	if entry.QoS != 1 {
		t.Errorf("QoS = %d, want 1", entry.QoS)
	}
	if entry.Priority != 5 {
		t.Errorf("Priority = %d, want 5", entry.Priority)
	}
	if entry.CreatedAt != 1000 {
		t.Errorf("CreatedAt = %d, want 1000", entry.CreatedAt)
	}
	if entry.ExpiresAt != 2000 {
		t.Errorf("ExpiresAt = %d, want 2000", entry.ExpiresAt)
	}
}

func TestHandlerRegistry_Dispatch(t *testing.T) {
	registry := NewHandlerRegistry()

	var mu sync.Mutex
	var receivedData []byte
	called := false

	registry.Register(MsgReplicateOffline, func(data []byte) {
		mu.Lock()
		defer mu.Unlock()
		called = true
		receivedData = make([]byte, len(data))
		copy(receivedData, data)
	})

	msg := ReplicateOfflineMessage{
		Type:     MsgReplicateOffline,
		ClientID: "client-1",
		Messages: []ReplicateOfflineEntry{
			{Topic: "t/1", Payload: []byte("hello"), QoS: 0},
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Dispatch to registered handler — should return true.
	if !registry.Dispatch(MsgReplicateOffline, data) {
		t.Fatal("Dispatch returned false for registered type")
	}

	mu.Lock()
	if !called {
		t.Fatal("handler was not called")
	}
	var got ReplicateOfflineMessage
	if err := json.Unmarshal(receivedData, &got); err != nil {
		t.Fatalf("unmarshal received data: %v", err)
	}
	mu.Unlock()

	if got.ClientID != "client-1" {
		t.Errorf("ClientID = %q, want %q", got.ClientID, "client-1")
	}

	// Dispatch unregistered type — should return false.
	if registry.Dispatch(MsgDeleteWill, data) {
		t.Fatal("Dispatch returned true for unregistered type")
	}
}
