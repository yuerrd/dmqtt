package cluster

import (
	"testing"
)

func TestLWWResolver_HigherEpochWins(t *testing.T) {
	r := &LWWResolver{}
	local := &SessionMeta{ClientID: "c1", Epoch: 1, ConnectTimestamp: 100}
	remote := &SessionMeta{ClientID: "c1", Epoch: 2, ConnectTimestamp: 50}

	remoteWins := r.Resolve(local, remote)
	if !remoteWins {
		t.Fatalf("expected remote to win (higher epoch), got local wins")
	}
}

func TestLWWResolver_SameEpoch_NewerTimestampWins(t *testing.T) {
	r := &LWWResolver{}
	local := &SessionMeta{ClientID: "c1", Epoch: 1, ConnectTimestamp: 100}
	remote := &SessionMeta{ClientID: "c1", Epoch: 1, ConnectTimestamp: 200}

	remoteWins := r.Resolve(local, remote)
	if !remoteWins {
		t.Fatalf("expected remote to win (newer timestamp), got local wins")
	}
}

func TestLWWResolver_SameEpochAndTimestamp_LocalWins(t *testing.T) {
	r := &LWWResolver{}
	local := &SessionMeta{ClientID: "c1", Epoch: 1, ConnectTimestamp: 100}
	remote := &SessionMeta{ClientID: "c1", Epoch: 1, ConnectTimestamp: 100}

	remoteWins := r.Resolve(local, remote)
	if remoteWins {
		t.Fatalf("expected local to win on tie, got remote wins")
	}
}
