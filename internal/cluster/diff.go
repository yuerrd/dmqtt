package cluster

// DiffOwnership compares old and new rings and returns which devices changed owner.
// Returns map[fromNodeID]map[toNodeID][]deviceID.
// Only includes devices whose primary owner changed.
func DiffOwnership(oldRing, newRing *Ring, deviceIDs []string) map[string]map[string][]string {
	result := make(map[string]map[string][]string)

	for _, deviceID := range deviceIDs {
		oldOwner, oldOK := oldRing.LocateDevice(deviceID)
		newOwner, newOK := newRing.LocateDevice(deviceID)
		if !oldOK || !newOK {
			continue
		}
		if oldOwner.ID == newOwner.ID {
			continue
		}

		if result[oldOwner.ID] == nil {
			result[oldOwner.ID] = make(map[string][]string)
		}
		result[oldOwner.ID][newOwner.ID] = append(result[oldOwner.ID][newOwner.ID], deviceID)
	}

	return result
}
