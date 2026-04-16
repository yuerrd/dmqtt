package cluster

import (
	"testing"
	"time"
)

func TestCluster_SingleNode(t *testing.T) {
	cfg := ClusterConfig{
		Enabled:       true,
		NodeID:        "test-node-1",
		Host:          "127.0.0.1",
		GossipPort:    18001,
		TransportPort: 29001,
		MQTTPort:      19001,
		VirtualNodes:  150,
		ReplicaCount:  3,
	}

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer c.Stop()

	if c.Self().ID != "test-node-1" {
		t.Errorf("Self().ID = %q", c.Self().ID)
	}

	if c.Size() != 1 {
		t.Errorf("Size() = %d, want 1", c.Size())
	}

	// Single node owns all devices
	if !c.IsLocal("any-device") {
		t.Error("single node should own all devices")
	}
}

func TestCluster_ThreeNodes_DeviceOwnership(t *testing.T) {
	cfg1 := ClusterConfig{NodeID: "c-node-1", Host: "127.0.0.1", GossipPort: 18002, TransportPort: 29002, MQTTPort: 19002, VirtualNodes: 150, ReplicaCount: 3}
	cfg2 := ClusterConfig{NodeID: "c-node-2", Host: "127.0.0.1", GossipPort: 18003, TransportPort: 29003, MQTTPort: 19003, VirtualNodes: 150, ReplicaCount: 3, Seeds: []string{"127.0.0.1:18002"}}
	cfg3 := ClusterConfig{NodeID: "c-node-3", Host: "127.0.0.1", GossipPort: 18004, TransportPort: 29004, MQTTPort: 19004, VirtualNodes: 150, ReplicaCount: 3, Seeds: []string{"127.0.0.1:18002"}}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	defer c1.Stop()

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	defer c2.Stop()

	c3, err := NewCluster(cfg3)
	if err != nil {
		t.Fatalf("c3: %v", err)
	}
	defer c3.Stop()

	// Wait for convergence
	time.Sleep(2 * time.Second)

	// All nodes should see 3 members
	for _, c := range []*Cluster{c1, c2, c3} {
		if c.Size() != 3 {
			t.Errorf("node %s sees %d members, want 3", c.Self().ID, c.Size())
		}
	}

	// All nodes should agree on device ownership
	for i := 0; i < 50; i++ {
		dev := "device-" + string(rune('A'+i%26))
		owner1, _ := c1.LocateDevice(dev)
		owner2, _ := c2.LocateDevice(dev)
		owner3, _ := c3.LocateDevice(dev)

		if owner1.ID != owner2.ID || owner2.ID != owner3.ID {
			t.Errorf("disagreement on %s: c1=%s c2=%s c3=%s", dev, owner1.ID, owner2.ID, owner3.ID)
		}
	}
}

func TestCluster_NodeLeave_RingUpdates(t *testing.T) {
	cfg1 := ClusterConfig{NodeID: "l-node-1", Host: "127.0.0.1", GossipPort: 18005, TransportPort: 29005, MQTTPort: 19005, VirtualNodes: 150, ReplicaCount: 3}
	cfg2 := ClusterConfig{NodeID: "l-node-2", Host: "127.0.0.1", GossipPort: 18006, TransportPort: 29006, MQTTPort: 19006, VirtualNodes: 150, ReplicaCount: 3, Seeds: []string{"127.0.0.1:18005"}}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	defer c1.Stop()

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}

	time.Sleep(time.Second)

	if c1.Size() != 2 {
		t.Fatalf("expected 2 nodes, got %d", c1.Size())
	}

	// Node 2 leaves
	c2.Stop()
	time.Sleep(2 * time.Second)

	// Node 1 should see only itself
	if c1.Size() != 1 {
		t.Errorf("after leave: expected 1 node, got %d", c1.Size())
	}

	// All devices should now be local to node 1
	if !c1.IsLocal("any-device") {
		t.Error("single remaining node should own all devices")
	}
}

func TestCluster_RequiresNodeID(t *testing.T) {
	cfg := ClusterConfig{Host: "127.0.0.1", GossipPort: 18007, TransportPort: 29007}
	_, err := NewCluster(cfg)
	if err == nil {
		t.Error("expected error for empty NodeID")
	}
}

func TestCluster_LocateDeviceN_Replicas(t *testing.T) {
	cfg1 := ClusterConfig{NodeID: "r-node-1", Host: "127.0.0.1", GossipPort: 18008, TransportPort: 29008, MQTTPort: 19008, VirtualNodes: 150, ReplicaCount: 3}
	cfg2 := ClusterConfig{NodeID: "r-node-2", Host: "127.0.0.1", GossipPort: 18009, TransportPort: 29009, MQTTPort: 19009, VirtualNodes: 150, ReplicaCount: 3, Seeds: []string{"127.0.0.1:18008"}}
	cfg3 := ClusterConfig{NodeID: "r-node-3", Host: "127.0.0.1", GossipPort: 18010, TransportPort: 29010, MQTTPort: 19010, VirtualNodes: 150, ReplicaCount: 3, Seeds: []string{"127.0.0.1:18008"}}

	c1, err := NewCluster(cfg1)
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	defer c1.Stop()

	c2, err := NewCluster(cfg2)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	defer c2.Stop()

	c3, err := NewCluster(cfg3)
	if err != nil {
		t.Fatalf("c3: %v", err)
	}
	defer c3.Stop()

	time.Sleep(2 * time.Second)

	// Default replica count = 3
	replicas := c1.LocateDeviceN("device-test", 0)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	// All distinct
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r.ID] {
			t.Errorf("duplicate replica: %s", r.ID)
		}
		seen[r.ID] = true
	}
}
