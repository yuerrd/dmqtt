package cluster

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestConnectionIndex_AddLookup(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-1", "node-A")
	nodeID, ok := idx.Lookup("device-1")
	if !ok {
		t.Fatal("expected device-1 to be found")
	}
	if nodeID != "node-A" {
		t.Errorf("nodeID = %s, want node-A", nodeID)
	}
}

func TestConnectionIndex_LookupMissing(t *testing.T) {
	idx := NewConnectionIndex()
	_, ok := idx.Lookup("nonexistent")
	if ok {
		t.Error("expected nonexistent device to not be found")
	}
}

func TestConnectionIndex_Remove(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-1", "node-A")
	idx.Remove("device-1")
	_, ok := idx.Lookup("device-1")
	if ok {
		t.Error("expected device-1 to be removed")
	}
	devices := idx.NodeDevices("node-A")
	if len(devices) != 0 {
		t.Errorf("expected 0 devices for node-A, got %d", len(devices))
	}
}

func TestConnectionIndex_AddOverwritesNode(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-1", "node-A")
	idx.Add("device-1", "node-B")
	nodeID, ok := idx.Lookup("device-1")
	if !ok || nodeID != "node-B" {
		t.Errorf("expected device-1 on node-B, got %s", nodeID)
	}
	devicesA := idx.NodeDevices("node-A")
	if len(devicesA) != 0 {
		t.Errorf("expected 0 devices for node-A, got %d", len(devicesA))
	}
	devicesB := idx.NodeDevices("node-B")
	if len(devicesB) != 1 || devicesB[0] != "device-1" {
		t.Errorf("expected [device-1] for node-B, got %v", devicesB)
	}
}

func TestConnectionIndex_RemoveNode(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-1", "node-A")
	idx.Add("device-2", "node-A")
	idx.Add("device-3", "node-B")
	idx.RemoveNode("node-A")
	_, ok1 := idx.Lookup("device-1")
	_, ok2 := idx.Lookup("device-2")
	nodeID3, ok3 := idx.Lookup("device-3")
	if ok1 || ok2 {
		t.Error("expected device-1 and device-2 to be removed")
	}
	if !ok3 || nodeID3 != "node-B" {
		t.Error("expected device-3 to remain on node-B")
	}
	if idx.Count() != 1 {
		t.Errorf("Count = %d, want 1", idx.Count())
	}
}

func TestConnectionIndex_NodeDevices(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-1", "node-A")
	idx.Add("device-2", "node-A")
	idx.Add("device-3", "node-B")
	devices := idx.NodeDevices("node-A")
	sort.Strings(devices)
	if len(devices) != 2 || devices[0] != "device-1" || devices[1] != "device-2" {
		t.Errorf("NodeDevices(node-A) = %v, want [device-1 device-2]", devices)
	}
	empty := idx.NodeDevices("nonexistent")
	if len(empty) != 0 {
		t.Errorf("expected empty for nonexistent node, got %v", empty)
	}
}

func TestConnectionIndex_Count(t *testing.T) {
	idx := NewConnectionIndex()
	if idx.Count() != 0 {
		t.Errorf("Count = %d, want 0", idx.Count())
	}
	idx.Add("device-1", "node-A")
	idx.Add("device-2", "node-B")
	if idx.Count() != 2 {
		t.Errorf("Count = %d, want 2", idx.Count())
	}
	idx.Remove("device-1")
	if idx.Count() != 1 {
		t.Errorf("Count = %d, want 1", idx.Count())
	}
}

func TestConnectionIndex_Concurrent(t *testing.T) {
	idx := NewConnectionIndex()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("device-%d", id)
			nodeID := fmt.Sprintf("node-%d", id%3)
			idx.Add(deviceID, nodeID)
			idx.Lookup(deviceID)
			idx.Count()
		}(i)
	}
	wg.Wait()
	if idx.Count() != 100 {
		t.Errorf("Count = %d, want 100", idx.Count())
	}
}

func TestConnectionIndex_MigratingState(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("dev-1", "node-1")

	// Not migrating by default
	if idx.IsMigrating("dev-1") {
		t.Fatal("should not be migrating by default")
	}

	// Mark as migrating
	idx.SetMigrating("dev-1", true)
	if !idx.IsMigrating("dev-1") {
		t.Fatal("should be migrating after SetMigrating(true)")
	}

	// Clear migrating
	idx.SetMigrating("dev-1", false)
	if idx.IsMigrating("dev-1") {
		t.Fatal("should not be migrating after SetMigrating(false)")
	}
}

func TestConnectionIndex_MigratingDevices(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("dev-1", "node-1")
	idx.Add("dev-2", "node-1")
	idx.Add("dev-3", "node-2")

	idx.SetMigrating("dev-1", true)
	idx.SetMigrating("dev-3", true)

	migrating := idx.MigratingDevices()
	if len(migrating) != 2 {
		t.Fatalf("expected 2 migrating devices, got %d", len(migrating))
	}
}

func TestConnectionIndex_ClearMigrating(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("dev-1", "node-1")
	idx.Add("dev-2", "node-1")

	idx.SetMigrating("dev-1", true)
	idx.SetMigrating("dev-2", true)
	idx.ClearAllMigrating()

	if idx.IsMigrating("dev-1") || idx.IsMigrating("dev-2") {
		t.Fatal("all migrating flags should be cleared")
	}
}