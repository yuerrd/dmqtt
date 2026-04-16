package cluster

import (
	"strings"
	"sync"
)

// RemoteMatch represents a remote node with matching subscribers.
type RemoteMatch struct {
	NodeID string
	MaxQoS byte
}

// RemoteSubIndex tracks which topic filters have subscribers on which remote nodes.
// Thread-safe.
type RemoteSubIndex struct {
	mu sync.RWMutex
	// topicFilter → map[nodeID]maxQoS
	subs map[string]map[string]byte
	// nodeID → set of topicFilters (for fast RemoveNode)
	nodes map[string]map[string]struct{}
}

// NewRemoteSubIndex creates a new empty RemoteSubIndex.
func NewRemoteSubIndex() *RemoteSubIndex {
	return &RemoteSubIndex{
		subs:  make(map[string]map[string]byte),
		nodes: make(map[string]map[string]struct{}),
	}
}

// Add registers a remote subscription. If the node already has a subscription
// for this filter, the QoS is updated to the higher value.
func (r *RemoteSubIndex) Add(nodeID, topicFilter string, qos byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subs[topicFilter] == nil {
		r.subs[topicFilter] = make(map[string]byte)
	}
	if existing, ok := r.subs[topicFilter][nodeID]; !ok || qos > existing {
		r.subs[topicFilter][nodeID] = qos
	}

	if r.nodes[nodeID] == nil {
		r.nodes[nodeID] = make(map[string]struct{})
	}
	r.nodes[nodeID][topicFilter] = struct{}{}
}

// Remove removes a remote subscription for a specific node and filter.
func (r *RemoteSubIndex) Remove(nodeID, topicFilter string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if nodeMap, ok := r.subs[topicFilter]; ok {
		delete(nodeMap, nodeID)
		if len(nodeMap) == 0 {
			delete(r.subs, topicFilter)
		}
	}

	if filters, ok := r.nodes[nodeID]; ok {
		delete(filters, topicFilter)
		if len(filters) == 0 {
			delete(r.nodes, nodeID)
		}
	}
}

// RemoveNode removes all subscriptions for a node (called on node leave).
func (r *RemoteSubIndex) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	filters, ok := r.nodes[nodeID]
	if !ok {
		return
	}

	for filter := range filters {
		if nodeMap, ok := r.subs[filter]; ok {
			delete(nodeMap, nodeID)
			if len(nodeMap) == 0 {
				delete(r.subs, filter)
			}
		}
	}

	delete(r.nodes, nodeID)
}

// topicMatch checks if a topic name matches an MQTT subscription filter.
// Implements MQTT 3.1.1 §4.7 wildcard matching rules.
func topicMatch(filter, topic string) bool {
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) > 0 && (filter[0] == '+' || filter[0] == '#') {
			return false
		}
	}

	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	for i := 0; i < len(filterParts); i++ {
		if filterParts[i] == "#" {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if filterParts[i] == "+" {
			continue
		}
		if filterParts[i] != topicParts[i] {
			return false
		}
	}

	return len(filterParts) == len(topicParts)
}

// Match finds which remote nodes have subscribers matching the given topic.
// Deduplicates by nodeID, keeping the highest QoS across matching filters.
func (r *RemoteSubIndex) Match(topic string) []RemoteMatch {
	r.mu.RLock()
	defer r.mu.RUnlock()

	best := make(map[string]byte)

	for filter, nodeMap := range r.subs {
		if topicMatch(filter, topic) {
			for nodeID, qos := range nodeMap {
				if existing, ok := best[nodeID]; !ok || qos > existing {
					best[nodeID] = qos
				}
			}
		}
	}

	result := make([]RemoteMatch, 0, len(best))
	for nodeID, qos := range best {
		result = append(result, RemoteMatch{NodeID: nodeID, MaxQoS: qos})
	}
	return result
}
