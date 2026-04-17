package cluster

import "sync"

// ConnectionIndex tracks which devices are connected to which nodes.
type ConnectionIndex struct {
	mu        sync.RWMutex
	devices   map[string]string              // deviceID → nodeID
	byNode    map[string]map[string]struct{} // nodeID → set of deviceIDs
	migrating map[string]bool                // deviceID → migrating flag
}

// NewConnectionIndex creates an empty ConnectionIndex.
func NewConnectionIndex() *ConnectionIndex {
	return &ConnectionIndex{
		devices:   make(map[string]string),
		byNode:    make(map[string]map[string]struct{}),
		migrating: make(map[string]bool),
	}
}

// Add registers a device as connected to a node.
// If the device was previously on a different node, it is moved.
func (idx *ConnectionIndex) Add(deviceID, nodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if prev, ok := idx.devices[deviceID]; ok && prev != nodeID {
		if devs, ok := idx.byNode[prev]; ok {
			delete(devs, deviceID)
			if len(devs) == 0 {
				delete(idx.byNode, prev)
			}
		}
	}

	idx.devices[deviceID] = nodeID

	if idx.byNode[nodeID] == nil {
		idx.byNode[nodeID] = make(map[string]struct{})
	}
	idx.byNode[nodeID][deviceID] = struct{}{}
}

// Remove removes a device from the index.
func (idx *ConnectionIndex) Remove(deviceID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	nodeID, ok := idx.devices[deviceID]
	if !ok {
		return
	}

	delete(idx.devices, deviceID)

	if devs, ok := idx.byNode[nodeID]; ok {
		delete(devs, deviceID)
		if len(devs) == 0 {
			delete(idx.byNode, nodeID)
		}
	}
}

// RemoveNode removes all devices for a node.
func (idx *ConnectionIndex) RemoveNode(nodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	devs, ok := idx.byNode[nodeID]
	if !ok {
		return
	}

	for deviceID := range devs {
		delete(idx.devices, deviceID)
	}
	delete(idx.byNode, nodeID)
}

// Lookup returns the node a device is connected to.
func (idx *ConnectionIndex) Lookup(deviceID string) (nodeID string, ok bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	nodeID, ok = idx.devices[deviceID]
	return
}

// NodeDevices returns all devices connected to a given node.
func (idx *ConnectionIndex) NodeDevices(nodeID string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	devs, ok := idx.byNode[nodeID]
	if !ok {
		return nil
	}

	result := make([]string, 0, len(devs))
	for d := range devs {
		result = append(result, d)
	}
	return result
}

// Count returns the total number of tracked devices.
func (idx *ConnectionIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.devices)
}

// SetMigrating marks a device as migrating or clears the flag.
func (idx *ConnectionIndex) SetMigrating(deviceID string, migrating bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if migrating {
		idx.migrating[deviceID] = true
	} else {
		delete(idx.migrating, deviceID)
	}
}

// IsMigrating returns true if the device is marked as migrating.
func (idx *ConnectionIndex) IsMigrating(deviceID string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.migrating[deviceID]
}

// MigratingDevices returns all device IDs currently marked as migrating.
func (idx *ConnectionIndex) MigratingDevices() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	result := make([]string, 0, len(idx.migrating))
	for d := range idx.migrating {
		result = append(result, d)
	}
	return result
}

// ClearAllMigrating removes all migrating flags.
func (idx *ConnectionIndex) ClearAllMigrating() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.migrating = make(map[string]bool)
}