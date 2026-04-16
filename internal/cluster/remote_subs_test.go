package cluster

import (
	"sort"
	"testing"
)

func TestRemoteSubIndex_AddAndMatch(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/temp", 1)
	idx.Add("node-c", "sensor/#", 2)

	matches := idx.Match("sensor/temp")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].NodeID < matches[j].NodeID
	})

	if matches[0].NodeID != "node-b" || matches[0].MaxQoS != 1 {
		t.Errorf("match[0] = %+v, want {node-b, 1}", matches[0])
	}
	if matches[1].NodeID != "node-c" || matches[1].MaxQoS != 2 {
		t.Errorf("match[1] = %+v, want {node-c, 2}", matches[1])
	}
}

func TestRemoteSubIndex_AddUpgradesQoS(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/temp", 0)
	idx.Add("node-b", "sensor/temp", 2)

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].MaxQoS != 2 {
		t.Errorf("MaxQoS = %d, want 2", matches[0].MaxQoS)
	}
}

func TestRemoteSubIndex_Remove(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/temp", 1)
	idx.Add("node-c", "sensor/temp", 2)
	idx.Remove("node-b", "sensor/temp")

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].NodeID != "node-c" {
		t.Errorf("NodeID = %s, want node-c", matches[0].NodeID)
	}
}

func TestRemoteSubIndex_RemoveNode(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/temp", 1)
	idx.Add("node-b", "sensor/humidity", 0)
	idx.Add("node-c", "sensor/temp", 2)

	idx.RemoveNode("node-b")

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].NodeID != "node-c" {
		t.Errorf("NodeID = %s, want node-c", matches[0].NodeID)
	}

	matches = idx.Match("sensor/humidity")
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(matches))
	}
}

func TestRemoteSubIndex_MatchNoResults(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/temp", 1)

	matches := idx.Match("light/brightness")
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(matches))
	}
}

func TestRemoteSubIndex_WildcardMatch(t *testing.T) {
	idx := NewRemoteSubIndex()

	idx.Add("node-b", "sensor/#", 1)
	idx.Add("node-c", "+/temp", 0)

	matches := idx.Match("sensor/temp")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}
}

func TestRemoteSubIndex_DeduplicatesNodePerTopic(t *testing.T) {
	idx := NewRemoteSubIndex()

	// Same node matches via two different filters
	idx.Add("node-b", "sensor/#", 1)
	idx.Add("node-b", "+/temp", 2)

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (deduplicated), got %d", len(matches))
	}
	// Should keep highest QoS across matching filters
	if matches[0].MaxQoS != 2 {
		t.Errorf("MaxQoS = %d, want 2 (max across filters)", matches[0].MaxQoS)
	}
}
