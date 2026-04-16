package cluster

import (
	"sync"

	"github.com/serialx/hashring"
)

// Ring provides consistent hashing for device-to-node mapping.
type Ring struct {
	mu           sync.RWMutex
	ring         *hashring.HashRing
	nodes        map[string]NodeInfo // nodeID → NodeInfo
	virtualNodes int
}

// NewRing creates a Ring with the given number of virtual nodes per physical node.
func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 150
	}
	return &Ring{
		ring:         hashring.New(nil),
		nodes:        make(map[string]NodeInfo),
		virtualNodes: virtualNodes,
	}
}

// Update rebuilds the ring with the given set of nodes.
func (r *Ring) Update(nodes []NodeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, len(nodes))
	r.nodes = make(map[string]NodeInfo, len(nodes))
	for i, n := range nodes {
		names[i] = n.ID
		r.nodes[n.ID] = n
	}

	weights := make(map[string]int, len(nodes))
	for _, name := range names {
		weights[name] = r.virtualNodes
	}
	r.ring = hashring.NewWithWeights(weights)
}

// LocateDevice returns the primary node responsible for the given device ID.
func (r *Ring) LocateDevice(deviceID string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodeID, ok := r.ring.GetNode(deviceID)
	if !ok {
		return NodeInfo{}, false
	}
	info, exists := r.nodes[nodeID]
	return info, exists
}

// LocateDeviceN returns up to n distinct nodes for the given device ID (primary + replicas).
func (r *Ring) LocateDeviceN(deviceID string, n int) []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodeIDs, ok := r.ring.GetNodes(deviceID, n)
	if !ok {
		return nil
	}

	result := make([]NodeInfo, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if info, exists := r.nodes[id]; exists {
			result = append(result, info)
		}
	}
	return result
}

// IsLocal returns true if the primary node for deviceID matches selfID.
func (r *Ring) IsLocal(deviceID string, selfID string) bool {
	primary, ok := r.LocateDevice(deviceID)
	if !ok {
		return true // no ring = standalone, treat everything as local
	}
	return primary.ID == selfID
}

// Size returns the number of physical nodes in the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
