package cluster

// SessionMeta holds session metadata for conflict resolution.
type SessionMeta struct {
	ClientID         string `json:"clientID"`
	Epoch            uint64 `json:"epoch"`
	ConnectTimestamp int64  `json:"connectTimestamp"`
	NodeID           string `json:"nodeID"`
}

// ConflictResolver decides which session wins in a conflict.
type ConflictResolver interface {
	Resolve(local, remote *SessionMeta) *SessionMeta
}

// LWWResolver implements Last-Writer-Wins: higher Epoch wins,
// then newer ConnectTimestamp, then local wins on tie.
type LWWResolver struct{}

func (r *LWWResolver) Resolve(local, remote *SessionMeta) *SessionMeta {
	if remote.Epoch > local.Epoch {
		return remote
	}
	if local.Epoch > remote.Epoch {
		return local
	}
	if remote.ConnectTimestamp > local.ConnectTimestamp {
		return remote
	}
	return local
}
