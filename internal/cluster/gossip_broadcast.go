package cluster

import (
	"encoding/json"
	"log/slog"

	"github.com/hashicorp/memberlist"
)

// SubBroadcast is the gossip message for subscription changes.
type SubBroadcast struct {
	Type        string `json:"type"`        // "sub" or "unsub"
	NodeID      string `json:"nodeID"`
	TopicFilter string `json:"topicFilter"`
	QoS         byte   `json:"qos,omitempty"`
}

// subBroadcastItem implements memberlist.Broadcast.
type subBroadcastItem struct {
	msg SubBroadcast
}

func (b *subBroadcastItem) Invalidates(other memberlist.Broadcast) bool {
	otherItem, ok := other.(*subBroadcastItem)
	if !ok {
		return false
	}
	// A newer broadcast for the same node+filter invalidates the older one
	return b.msg.NodeID == otherItem.msg.NodeID &&
		b.msg.TopicFilter == otherItem.msg.TopicFilter
}

func (b *subBroadcastItem) Message() []byte {
	data, err := json.Marshal(b.msg)
	if err != nil {
		slog.Error("gossip: marshal broadcast error", "error", err)
		return nil
	}
	return data
}

func (b *subBroadcastItem) Finished() {}

// HandleSubBroadcast processes an incoming subscription broadcast and updates
// the remote subscription index.
func HandleSubBroadcast(idx *RemoteSubIndex, msg SubBroadcast) {
	switch msg.Type {
	case "sub":
		idx.Add(msg.NodeID, msg.TopicFilter, msg.QoS)
	case "unsub":
		idx.Remove(msg.NodeID, msg.TopicFilter)
	default:
		slog.Warn("gossip: unknown broadcast type", "type", msg.Type)
	}
}

// ConnBroadcast is the gossip message for connection changes.
type ConnBroadcast struct {
	Type     string `json:"type"`     // "conn" or "disconn"
	NodeID   string `json:"nodeID"`
	DeviceID string `json:"deviceID"`
}

// connBroadcastItem implements memberlist.Broadcast.
type connBroadcastItem struct {
	msg ConnBroadcast
}

func (b *connBroadcastItem) Invalidates(other memberlist.Broadcast) bool {
	otherItem, ok := other.(*connBroadcastItem)
	if !ok {
		return false
	}
	return b.msg.DeviceID == otherItem.msg.DeviceID
}

func (b *connBroadcastItem) Message() []byte {
	data, err := json.Marshal(b.msg)
	if err != nil {
		slog.Error("gossip: marshal conn broadcast error", "error", err)
		return nil
	}
	return data
}

func (b *connBroadcastItem) Finished() {}

// HandleConnBroadcast processes an incoming connection broadcast and updates
// the connection index.
func HandleConnBroadcast(idx *ConnectionIndex, msg ConnBroadcast) {
	switch msg.Type {
	case "conn":
		idx.Add(msg.DeviceID, msg.NodeID)
	case "disconn":
		idx.Remove(msg.DeviceID)
	default:
		slog.Warn("gossip: unknown conn broadcast type", "type", msg.Type)
	}
}
