package cluster

import (
	"sync"
	"testing"
	"time"
)

func TestIntegration_CrossNodeForward(t *testing.T) {
	cfg1 := ClusterConfig{
		NodeID:        "node-1",
		Host:          "127.0.0.1",
		GossipPort:    20001,
		TransportPort: 21001,
		MQTTPort:      1883,
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("NewCluster node-1: %v", err)
	}
	defer c1.Stop()

	cfg2 := ClusterConfig{
		NodeID:        "node-2",
		Host:          "127.0.0.1",
		GossipPort:    20002,
		TransportPort: 21002,
		MQTTPort:      1884,
		Seeds:         []string{"127.0.0.1:20001"},
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("NewCluster node-2: %v", err)
	}
	defer c2.Stop()

	time.Sleep(2 * time.Second)

	var received ForwardMessage
	var mu sync.Mutex
	done := make(chan struct{})

	c1.SetForwardHandler(func(msg ForwardMessage) {
		mu.Lock()
		received = msg
		mu.Unlock()
		close(done)
	})

	msg := ForwardMessage{
		Topic:   "sensor/temp",
		Payload: []byte("23.5"),
		QoS:     0,
		Retain:  false,
	}
	err = c2.Forward("node-1", msg)
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for forwarded message")
	}

	mu.Lock()
	defer mu.Unlock()
	if received.Topic != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", received.Topic, "sensor/temp")
	}
	if string(received.Payload) != "23.5" {
		t.Errorf("Payload = %q, want %q", received.Payload, "23.5")
	}
	if !received.Forwarded {
		t.Error("Forwarded should be true")
	}
}

func TestIntegration_SubscriptionGossip(t *testing.T) {
	cfg1 := ClusterConfig{
		NodeID:        "node-1",
		Host:          "127.0.0.1",
		GossipPort:    20003,
		TransportPort: 21003,
		MQTTPort:      1883,
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("NewCluster node-1: %v", err)
	}
	defer c1.Stop()

	cfg2 := ClusterConfig{
		NodeID:        "node-2",
		Host:          "127.0.0.1",
		GossipPort:    20004,
		TransportPort: 21004,
		MQTTPort:      1884,
		Seeds:         []string{"127.0.0.1:20003"},
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("NewCluster node-2: %v", err)
	}
	defer c2.Stop()

	time.Sleep(2 * time.Second)

	c1.BroadcastSubscribe("sensor/temp", 1)

	time.Sleep(3 * time.Second)

	matches := c2.RemoteSubs().Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 remote match, got %d", len(matches))
	}
	if matches[0].NodeID != "node-1" {
		t.Errorf("NodeID = %s, want node-1", matches[0].NodeID)
	}
	if matches[0].MaxQoS != 1 {
		t.Errorf("MaxQoS = %d, want 1", matches[0].MaxQoS)
	}
}

func TestIntegration_ForwardedMessageNotReforwarded(t *testing.T) {
	cfg1 := ClusterConfig{
		NodeID:        "node-1",
		Host:          "127.0.0.1",
		GossipPort:    20005,
		TransportPort: 21005,
		MQTTPort:      1883,
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("NewCluster node-1: %v", err)
	}
	defer c1.Stop()

	msg := ForwardMessage{
		Topic:   "test/topic",
		Payload: []byte("data"),
		QoS:     0,
	}

	done := make(chan ForwardMessage, 1)
	c1.SetForwardHandler(func(m ForwardMessage) {
		done <- m
	})

	if msg.Forwarded {
		t.Error("Original message should not have Forwarded set")
	}
}
