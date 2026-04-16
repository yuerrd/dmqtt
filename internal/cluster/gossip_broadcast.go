package cluster

import (
	"encoding/json"
	"log"

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
		log.Printf("gossip: marshal broadcast error: %v", err)
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
		log.Printf("gossip: unknown broadcast type: %s", msg.Type)
	}
}
