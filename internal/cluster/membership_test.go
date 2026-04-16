package cluster

import (
	"testing"
	"time"
)

func TestMembership_SingleNode(t *testing.T) {
	self := NodeInfo{
		ID:         "node-1",
		Host:       "127.0.0.1",
		GossipPort: 17001,
		MQTTPort:   11883,
	}

	m, err := NewMembership(self, nil)
	if err != nil {
		t.Fatalf("NewMembership: %v", err)
	}
	defer m.Leave(time.Second)

	members := m.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].ID != "node-1" {
		t.Errorf("member ID = %q, want %q", members[0].ID, "node-1")
	}

	if m.Self().ID != "node-1" {
		t.Errorf("Self().ID = %q", m.Self().ID)
	}
}

func TestMembership_TwoNodes_JoinAndLeave(t *testing.T) {
	node1 := NodeInfo{ID: "node-1", Host: "127.0.0.1", GossipPort: 17002, MQTTPort: 11884}
	node2 := NodeInfo{ID: "node-2", Host: "127.0.0.1", GossipPort: 17003, MQTTPort: 11885}

	m1, err := NewMembership(node1, nil)
	if err != nil {
		t.Fatalf("m1: %v", err)
	}
	defer m1.Leave(time.Second)

	m2, err := NewMembership(node2, []string{"127.0.0.1:17002"})
	if err != nil {
		t.Fatalf("m2: %v", err)
	}
	defer m2.Leave(time.Second)

	// Wait for convergence
	time.Sleep(500 * time.Millisecond)

	members1 := m1.Members()
	members2 := m2.Members()

	if len(members1) != 2 {
		t.Errorf("m1 sees %d members, want 2", len(members1))
	}
	if len(members2) != 2 {
		t.Errorf("m2 sees %d members, want 2", len(members2))
	}

	// Check join events on m1 (should have gotten node-2 join)
	// Note: self-join event may fire first, so drain until we find node-2
	foundNode2 := false
	timeout := time.After(2 * time.Second)
	for !foundNode2 {
		select {
		case ev := <-m1.Events():
			if ev.Type == NodeJoin && ev.Node.ID == "node-2" {
				foundNode2 = true
			}
		case <-timeout:
			t.Error("timeout waiting for node-2 join event on m1")
			return
		}
	}
}

func TestMembership_ThreeNodes(t *testing.T) {
	node1 := NodeInfo{ID: "node-1", Host: "127.0.0.1", GossipPort: 17004, MQTTPort: 11886}
	node2 := NodeInfo{ID: "node-2", Host: "127.0.0.1", GossipPort: 17005, MQTTPort: 11887}
	node3 := NodeInfo{ID: "node-3", Host: "127.0.0.1", GossipPort: 17006, MQTTPort: 11888}

	m1, err := NewMembership(node1, nil)
	if err != nil {
		t.Fatalf("m1: %v", err)
	}
	defer m1.Leave(time.Second)

	m2, err := NewMembership(node2, []string{"127.0.0.1:17004"})
	if err != nil {
		t.Fatalf("m2: %v", err)
	}
	defer m2.Leave(time.Second)

	m3, err := NewMembership(node3, []string{"127.0.0.1:17004"})
	if err != nil {
		t.Fatalf("m3: %v", err)
	}
	defer m3.Leave(time.Second)

	time.Sleep(time.Second)

	for _, m := range []*Membership{m1, m2, m3} {
		members := m.Members()
		if len(members) != 3 {
			t.Errorf("%s sees %d members, want 3", m.Self().ID, len(members))
		}
	}
}
