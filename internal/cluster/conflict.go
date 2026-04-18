package cluster

// SessionMeta holds session metadata for conflict resolution.
type SessionMeta struct {
	ClientID         string `json:"clientID"`
	Epoch            uint64 `json:"epoch"`
	ConnectTimestamp int64  `json:"connectTimestamp"`
	NodeID           string `json:"nodeID"`
}

// ConflictResolver determines session conflict outcomes.
// Resolve returns true if remote wins (local should yield).
type ConflictResolver interface {
	Resolve(local, remote *SessionMeta) bool
}

// LWWResolver implements Last-Writer-Wins: higher Epoch wins,
// then newer ConnectTimestamp, then local wins on tie.
type LWWResolver struct{}

func (r *LWWResolver) Resolve(local, remote *SessionMeta) bool {
	if remote.Epoch > local.Epoch {
		return true
	}
	if remote.Epoch < local.Epoch {
		return false
	}
	if remote.ConnectTimestamp > local.ConnectTimestamp {
		return true
	}
	if remote.ConnectTimestamp < local.ConnectTimestamp {
		return false
	}
	// Same epoch and timestamp: local wins (returns false = remote does NOT win)
	return false
}
