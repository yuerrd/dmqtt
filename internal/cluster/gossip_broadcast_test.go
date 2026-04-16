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

func TestConnBroadcastItem_Invalidates_SameDevice(t *testing.T) {
	b1 := &connBroadcastItem{msg: ConnBroadcast{Type: "conn", NodeID: "node-1", DeviceID: "device-A"}}
	b2 := &connBroadcastItem{msg: ConnBroadcast{Type: "disconn", NodeID: "node-1", DeviceID: "device-A"}}
	if !b1.Invalidates(b2) {
		t.Error("expected same deviceID to invalidate")
	}
}

func TestConnBroadcastItem_Invalidates_DifferentDevice(t *testing.T) {
	b1 := &connBroadcastItem{msg: ConnBroadcast{Type: "conn", NodeID: "node-1", DeviceID: "device-A"}}
	b2 := &connBroadcastItem{msg: ConnBroadcast{Type: "conn", NodeID: "node-1", DeviceID: "device-B"}}
	if b1.Invalidates(b2) {
		t.Error("expected different deviceID to NOT invalidate")
	}
}

func TestConnBroadcastItem_Invalidates_DifferentType(t *testing.T) {
	conn := &connBroadcastItem{msg: ConnBroadcast{Type: "conn", NodeID: "node-1", DeviceID: "device-A"}}
	sub := &subBroadcastItem{msg: SubBroadcast{Type: "sub", NodeID: "node-1", TopicFilter: "a/b"}}
	if conn.Invalidates(sub) {
		t.Error("expected connBroadcast to NOT invalidate subBroadcast")
	}
}

func TestHandleConnBroadcast_ConnAdds(t *testing.T) {
	idx := NewConnectionIndex()
	HandleConnBroadcast(idx, ConnBroadcast{Type: "conn", NodeID: "node-1", DeviceID: "device-A"})
	nodeID, ok := idx.Lookup("device-A")
	if !ok || nodeID != "node-1" {
		t.Errorf("expected device-A on node-1, got %s, ok=%v", nodeID, ok)
	}
}

func TestHandleConnBroadcast_DisconnRemoves(t *testing.T) {
	idx := NewConnectionIndex()
	idx.Add("device-A", "node-1")
	HandleConnBroadcast(idx, ConnBroadcast{Type: "disconn", NodeID: "node-1", DeviceID: "device-A"})
	_, ok := idx.Lookup("device-A")
	if ok {
		t.Error("expected device-A to be removed")
	}
}
