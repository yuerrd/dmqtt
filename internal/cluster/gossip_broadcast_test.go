package cluster

import (
	"encoding/json"
	"testing"
)

func TestSubBroadcast_SerializeDeserialize(t *testing.T) {
	original := SubBroadcast{
		Type:        "sub",
		NodeID:      "node-a",
		TopicFilter: "sensor/temp",
		QoS:         1,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded SubBroadcast
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded != original {
		t.Errorf("decoded = %+v, want %+v", decoded, original)
	}
}

func TestSubBroadcastItem_ImplementsBroadcast(t *testing.T) {
	item := &subBroadcastItem{
		msg: SubBroadcast{
			Type:        "sub",
			NodeID:      "node-a",
			TopicFilter: "sensor/temp",
			QoS:         1,
		},
	}

	data := item.Message()
	if len(data) == 0 {
		t.Fatal("Message() returned empty")
	}

	// An "unsub" for same node+filter should invalidate a "sub"
	item2 := &subBroadcastItem{
		msg: SubBroadcast{
			Type:        "unsub",
			NodeID:      "node-a",
			TopicFilter: "sensor/temp",
		},
	}

	if !item2.Invalidates(item) {
		t.Error("unsub should invalidate sub for same node+filter")
	}

	// Different filter should not invalidate
	item3 := &subBroadcastItem{
		msg: SubBroadcast{
			Type:        "unsub",
			NodeID:      "node-a",
			TopicFilter: "sensor/humidity",
		},
	}

	if item3.Invalidates(item) {
		t.Error("unsub for different filter should not invalidate")
	}
}

func TestHandleSubBroadcast(t *testing.T) {
	idx := NewRemoteSubIndex()

	// Simulate receiving a "sub" broadcast
	subMsg := SubBroadcast{
		Type:        "sub",
		NodeID:      "node-b",
		TopicFilter: "sensor/temp",
		QoS:         1,
	}
	HandleSubBroadcast(idx, subMsg)

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].NodeID != "node-b" {
		t.Errorf("NodeID = %s, want node-b", matches[0].NodeID)
	}

	// Simulate receiving an "unsub" broadcast
	unsubMsg := SubBroadcast{
		Type:        "unsub",
		NodeID:      "node-b",
		TopicFilter: "sensor/temp",
	}
	HandleSubBroadcast(idx, unsubMsg)

	matches = idx.Match("sensor/temp")
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches after unsub, got %d", len(matches))
	}
}
