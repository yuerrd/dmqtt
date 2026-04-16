package cluster

import "testing"

func TestRing_EmptyRing(t *testing.T) {
	r := NewRing(150)

	_, ok := r.LocateDevice("device-1")
	if ok {
		t.Error("expected no node for empty ring")
	}

	if !r.IsLocal("device-1", "any-node") {
		t.Error("empty ring should treat all devices as local")
	}
}

func TestRing_SingleNode(t *testing.T) {
	r := NewRing(150)
	r.Update([]NodeInfo{{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000, MQTTPort: 1883}})

	primary, ok := r.LocateDevice("device-abc")
	if !ok {
		t.Fatal("expected a node")
	}
	if primary.ID != "node-1" {
		t.Errorf("expected node-1, got %s", primary.ID)
	}

	if !r.IsLocal("device-abc", "node-1") {
		t.Error("single node should own everything")
	}
}

func TestRing_MultipleNodes_ConsistentMapping(t *testing.T) {
	r := NewRing(150)
	nodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000, MQTTPort: 1883},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000, MQTTPort: 1883},
		{ID: "node-3", Host: "10.0.0.3", GossipPort: 7000, MQTTPort: 1883},
	}
	r.Update(nodes)

	// Same device always maps to same node
	primary1, _ := r.LocateDevice("device-xyz")
	primary2, _ := r.LocateDevice("device-xyz")
	if primary1.ID != primary2.ID {
		t.Errorf("inconsistent: %s vs %s", primary1.ID, primary2.ID)
	}

	// Different devices should spread across nodes (probabilistic, test with many)
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		dev := "device-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		p, _ := r.LocateDevice(dev)
		counts[p.ID]++
	}
	for _, node := range nodes {
		if counts[node.ID] == 0 {
			t.Errorf("node %s got 0 devices out of 1000", node.ID)
		}
	}
}

func TestRing_LocateDeviceN_Replicas(t *testing.T) {
	r := NewRing(150)
	nodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000, MQTTPort: 1883},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000, MQTTPort: 1883},
		{ID: "node-3", Host: "10.0.0.3", GossipPort: 7000, MQTTPort: 1883},
	}
	r.Update(nodes)

	replicas := r.LocateDeviceN("device-1", 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	// All replicas should be distinct
	seen := make(map[string]bool)
	for _, n := range replicas {
		if seen[n.ID] {
			t.Errorf("duplicate replica: %s", n.ID)
		}
		seen[n.ID] = true
	}
}

func TestRing_NodeAddRemove_MinimalReassignment(t *testing.T) {
	r := NewRing(150)
	nodes2 := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000, MQTTPort: 1883},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000, MQTTPort: 1883},
	}
	r.Update(nodes2)

	// Record mappings with 2 nodes
	before := make(map[string]string)
	for i := 0; i < 100; i++ {
		dev := "dev-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		p, _ := r.LocateDevice(dev)
		before[dev] = p.ID
	}

	// Add a third node
	nodes3 := append(nodes2, NodeInfo{ID: "node-3", Host: "10.0.0.3", GossipPort: 7000, MQTTPort: 1883})
	r.Update(nodes3)

	// Count how many devices moved
	moved := 0
	for dev, oldNode := range before {
		p, _ := r.LocateDevice(dev)
		if p.ID != oldNode {
			moved++
		}
	}

	// With consistent hashing, roughly 1/3 should move (not all)
	if moved > 60 {
		t.Errorf("too many devices moved: %d/100 (expected ~33)", moved)
	}
}

func TestRing_Size(t *testing.T) {
	r := NewRing(150)
	if r.Size() != 0 {
		t.Errorf("empty ring size = %d", r.Size())
	}

	r.Update([]NodeInfo{
		{ID: "n1"}, {ID: "n2"}, {ID: "n3"},
	})
	if r.Size() != 3 {
		t.Errorf("size = %d, want 3", r.Size())
	}
}
