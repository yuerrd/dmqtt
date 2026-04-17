package cluster

import (
	"fmt"
	"testing"
)

func TestRing_Snapshot(t *testing.T) {
	r := NewRing(10)
	nodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
	}
	r.Update(nodes)

	snap := r.Snapshot()
	if snap == nil {
		t.Fatal("snapshot should not be nil")
	}

	// Snapshot should locate same device to same node
	primary, ok := r.LocateDevice("device-100")
	if !ok {
		t.Fatal("LocateDevice failed")
	}
	snapPrimary, ok := snap.LocateDevice("device-100")
	if !ok {
		t.Fatal("snapshot LocateDevice failed")
	}
	if primary.ID != snapPrimary.ID {
		t.Fatalf("snapshot and original disagree: %s vs %s", primary.ID, snapPrimary.ID)
	}
}

func TestDiffOwnership_NoChange(t *testing.T) {
	nodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
	}
	oldRing := NewRing(10)
	oldRing.Update(nodes)

	newRing := NewRing(10)
	newRing.Update(nodes)

	devices := []string{"dev-1", "dev-2", "dev-3", "dev-4", "dev-5"}
	diff := DiffOwnership(oldRing, newRing, devices)
	if len(diff) != 0 {
		t.Fatalf("expected no changes, got %d source nodes", len(diff))
	}
}

func TestDiffOwnership_NodeAdded(t *testing.T) {
	oldNodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
	}
	newNodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
		{ID: "node-3", Host: "10.0.0.3", GossipPort: 7000},
	}

	oldRing := NewRing(10)
	oldRing.Update(oldNodes)

	newRing := NewRing(10)
	newRing.Update(newNodes)

	// Generate enough devices so some will move
	var devices []string
	for i := 0; i < 100; i++ {
		devices = append(devices, fmt.Sprintf("dev-%d", i))
	}

	diff := DiffOwnership(oldRing, newRing, devices)

	// At least some devices should have moved to node-3
	totalMoved := 0
	for _, targets := range diff {
		for _, devs := range targets {
			totalMoved += len(devs)
		}
	}
	if totalMoved == 0 {
		t.Fatal("expected some devices to move when adding a node")
	}

	// All moved devices should have node-3 as the new owner
	hasNode3Target := false
	for _, targets := range diff {
		if _, ok := targets["node-3"]; ok {
			hasNode3Target = true
		}
	}
	if !hasNode3Target {
		t.Fatal("expected some devices to move to node-3")
	}
}

func TestDiffOwnership_NodeRemoved(t *testing.T) {
	oldNodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
		{ID: "node-3", Host: "10.0.0.3", GossipPort: 7000},
	}
	newNodes := []NodeInfo{
		{ID: "node-1", Host: "10.0.0.1", GossipPort: 7000},
		{ID: "node-2", Host: "10.0.0.2", GossipPort: 7000},
	}

	oldRing := NewRing(10)
	oldRing.Update(oldNodes)

	newRing := NewRing(10)
	newRing.Update(newNodes)

	var devices []string
	for i := 0; i < 100; i++ {
		devices = append(devices, fmt.Sprintf("dev-%d", i))
	}

	diff := DiffOwnership(oldRing, newRing, devices)

	// Devices that were on node-3 should now move to node-1 or node-2
	if _, ok := diff["node-3"]; !ok {
		t.Fatal("expected node-3 to be a source of migrations")
	}
}
