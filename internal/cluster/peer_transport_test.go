package cluster

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/langzp/dmqtt/internal/circuitbreaker"
)

func TestPeerTransport_SendReceive(t *testing.T) {
	var received ForwardMessage
	var mu sync.Mutex
	done := make(chan struct{})

	handler := func(msg ForwardMessage) {
		mu.Lock()
		received = msg
		mu.Unlock()
		close(done)
	}

	// Start receiver
	pt1, err := NewPeerTransport("127.0.0.1:19001", "node-1", handler)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}
	defer pt1.Stop()

	// Start sender
	pt2, err := NewPeerTransport("127.0.0.1:19002", "node-2", nil)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}
	defer pt2.Stop()

	pt2.AddPeer("node-1", "127.0.0.1:19001")
	time.Sleep(100 * time.Millisecond) // let connection establish

	msg := ForwardMessage{
		Type:    MsgForward,
		ID:      1,
		Topic:   "sensor/temp",
		Payload: []byte("23.5"),
		QoS:     0,
	}
	err = pt2.Send("node-1", msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	mu.Lock()
	defer mu.Unlock()
	if received.Topic != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", received.Topic, "sensor/temp")
	}
	if string(received.Payload) != "23.5" {
		t.Errorf("Payload = %q, want %q", received.Payload, "23.5")
	}
}

func TestPeerTransport_SendQoS1WithAck(t *testing.T) {
	handler := func(msg ForwardMessage) {
		// receiving side: PeerTransport auto-sends ACK for QoS > 0
	}

	pt1, err := NewPeerTransport("127.0.0.1:19003", "node-1", handler)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}
	defer pt1.Stop()

	pt2, err := NewPeerTransport("127.0.0.1:19004", "node-2", nil)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}
	defer pt2.Stop()

	pt2.AddPeer("node-1", "127.0.0.1:19003")
	time.Sleep(100 * time.Millisecond)

	msg := ForwardMessage{
		Type:    MsgForward,
		ID:      42,
		Topic:   "sensor/temp",
		Payload: []byte("23.5"),
		QoS:     1,
	}
	err = pt2.SendReliable("node-1", msg, 3, 2*time.Second)
	if err != nil {
		t.Fatalf("SendReliable: %v", err)
	}
}

func TestPeerTransport_RemovePeer(t *testing.T) {
	pt, err := NewPeerTransport("127.0.0.1:19005", "node-1", nil)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}
	defer pt.Stop()

	pt.AddPeer("node-2", "127.0.0.1:19999")
	pt.RemovePeer("node-2")

	err = pt.Send("node-2", ForwardMessage{})
	if err == nil {
		t.Fatal("expected error sending to removed peer")
	}
}

func TestPeerTransport_StopClosesConnections(t *testing.T) {
	pt, err := NewPeerTransport("127.0.0.1:19006", "node-1", nil)
	if err != nil {
		t.Fatalf("NewPeerTransport: %v", err)
	}

	pt.AddPeer("node-2", "127.0.0.1:19999")
	err = pt.Stop()
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPeerTransport_CircuitBreakerBlocksSend(t *testing.T) {
	// Start a receiver that accepts but never reads (simulates unresponsive peer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close() // close immediately to cause send failures
		}
	}()

	handler := func(msg ForwardMessage) {}
	pt, err := NewPeerTransport("127.0.0.1:0", "self", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Stop()

	// Configure a very aggressive breaker for testing
	cbCfg := circuitbreaker.Config{
		ErrorThreshold: 0.5,
		WindowSize:     5 * time.Second,
		OpenDuration:   10 * time.Second,
		HalfOpenMax:    2,
		MinRequests:    3,
	}
	pt.SetCircuitBreakerConfig(cbCfg)
	pt.AddPeer("peer1", ln.Addr().String())

	time.Sleep(100 * time.Millisecond) // let connection establish

	// Send several messages — they should fail and trip the breaker
	msg := ForwardMessage{Type: MsgForward, Topic: "test", Payload: []byte("hello")}
	for i := 0; i < 5; i++ {
		pt.Send("peer1", msg)
		time.Sleep(10 * time.Millisecond)
	}

	// Next send should return ErrCircuitOpen
	err = pt.Send("peer1", msg)
	if err != circuitbreaker.ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestPeerTransport_CircuitBreakerDefaultDisabled(t *testing.T) {
	handler := func(msg ForwardMessage) {}
	pt, err := NewPeerTransport("127.0.0.1:0", "self", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Stop()

	// Without SetCircuitBreakerConfig, breaker should not interfere
	// Send to nonexistent peer should return "peer not found", not circuit open
	err = pt.Send("nonexistent", ForwardMessage{})
	if err == nil {
		t.Fatal("expected error for nonexistent peer")
	}
	if err == circuitbreaker.ErrCircuitOpen {
		t.Fatal("should not get circuit open error when breaker is not configured")
	}
}

func TestPeerTransport_MigrateMessages(t *testing.T) {
	received := make(chan MigrateDataMessage, 1)

	// Start receiver node
	pt1, err := NewPeerTransport("127.0.0.1:0", "node-1", func(msg ForwardMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	pt1.SetMigrateHandler(func(msg MigrateDataMessage) {
		received <- msg
	})
	defer pt1.Stop()

	// Start sender node
	pt2, err := NewPeerTransport("127.0.0.1:0", "node-2", func(msg ForwardMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	defer pt2.Stop()

	pt2.AddPeer("node-1", pt1.listenAddr)
	time.Sleep(200 * time.Millisecond)

	// Send migration data
	migrateMsg := MigrateDataMessage{
		Type:     MsgMigrateData,
		DeviceID: "dev-1",
		Messages: []MigrateOfflineMsg{
			{Topic: "test/1", Payload: []byte("hello"), QoS: 1},
		},
		Session: &MigrateSessionData{
			ClientID:      "dev-1",
			CleanSession:  false,
			Subscriptions: map[string]byte{"test/#": 1},
		},
	}
	err = pt2.SendMigrate("node-1", migrateMsg)
	if err != nil {
		t.Fatalf("SendMigrate failed: %v", err)
	}

	select {
	case msg := <-received:
		if msg.DeviceID != "dev-1" {
			t.Fatalf("expected dev-1, got %s", msg.DeviceID)
		}
		if len(msg.Messages) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msg.Messages))
		}
		if msg.Session == nil {
			t.Fatal("expected session data")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for migrate message")
	}
}
