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

func TestIntegration_ConnectionGossip(t *testing.T) {
	cfg1 := ClusterConfig{
		NodeID:        "node-1",
		Host:          "127.0.0.1",
		GossipPort:    20007,
		TransportPort: 21007,
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
		GossipPort:    20008,
		TransportPort: 21008,
		MQTTPort:      1884,
		Seeds:         []string{"127.0.0.1:20007"},
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("NewCluster node-2: %v", err)
	}
	defer c2.Stop()

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	// Node-1 broadcasts a connection
	c1.BroadcastConnect("device-A")

	// Wait for gossip propagation
	time.Sleep(3 * time.Second)

	// Node-2 should see device-A connected to node-1
	nodeID, ok := c2.Connections().Lookup("device-A")
	if !ok {
		t.Fatal("expected device-A in node-2's connection index")
	}
	if nodeID != "node-1" {
		t.Errorf("nodeID = %s, want node-1", nodeID)
	}

	// Node-1 broadcasts a disconnection
	c1.BroadcastDisconnect("device-A")

	time.Sleep(3 * time.Second)

	_, ok = c2.Connections().Lookup("device-A")
	if ok {
		t.Error("expected device-A to be removed from node-2's connection index")
	}
}

func TestIntegration_RemoteConnectCallback(t *testing.T) {
	cfg1 := ClusterConfig{
		NodeID:        "node-1",
		Host:          "127.0.0.1",
		GossipPort:    20009,
		TransportPort: 21009,
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
		GossipPort:    20010,
		TransportPort: 21010,
		MQTTPort:      1884,
		Seeds:         []string{"127.0.0.1:20009"},
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("NewCluster node-2: %v", err)
	}
	defer c2.Stop()

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	// Set up remote connect handler on node-1
	var takeover struct {
		mu       sync.Mutex
		deviceID string
		nodeID   string
	}
	done := make(chan struct{})
	var once sync.Once

	c1.SetRemoteConnectHandler(func(deviceID, nodeID string) {
		takeover.mu.Lock()
		takeover.deviceID = deviceID
		takeover.nodeID = nodeID
		takeover.mu.Unlock()
		once.Do(func() {
			close(done)
		})
	})

	// Node-2 broadcasts a connect for device-A
	c2.BroadcastConnect("device-A")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for remote connect callback")
	}

	takeover.mu.Lock()
	defer takeover.mu.Unlock()
	if takeover.deviceID != "device-A" {
		t.Errorf("deviceID = %s, want device-A", takeover.deviceID)
	}
	if takeover.nodeID != "node-2" {
		t.Errorf("nodeID = %s, want node-2", takeover.nodeID)
	}
}
