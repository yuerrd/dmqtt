package cluster

import (
	"testing"
	"time"
)

func TestLWWResolver_HigherEpochWins(t *testing.T) {
	r := &LWWResolver{}
	now := time.Now().UnixNano()

	local := &SessionMeta{ClientID: "c1", Epoch: 3, ConnectTimestamp: now}
	remote := &SessionMeta{ClientID: "c1", Epoch: 5, ConnectTimestamp: now - 1000}

	winner := r.Resolve(local, remote)
	if winner != remote {
		t.Error("higher Epoch should win, regardless of timestamp")
	}
}

func TestLWWResolver_SameEpoch_NewerTimestampWins(t *testing.T) {
	r := &LWWResolver{}
	now := time.Now().UnixNano()

	local := &SessionMeta{ClientID: "c1", Epoch: 5, ConnectTimestamp: now - 1000}
	remote := &SessionMeta{ClientID: "c1", Epoch: 5, ConnectTimestamp: now}

	winner := r.Resolve(local, remote)
	if winner != remote {
		t.Error("same Epoch, newer timestamp should win")
	}
}

func TestLWWResolver_SameEpochAndTimestamp_LocalWins(t *testing.T) {
	r := &LWWResolver{}
	now := time.Now().UnixNano()

	local := &SessionMeta{ClientID: "c1", Epoch: 5, ConnectTimestamp: now}
	remote := &SessionMeta{ClientID: "c1", Epoch: 5, ConnectTimestamp: now}

	winner := r.Resolve(local, remote)
	if winner != local {
		t.Error("tie should favor local")
	}
}
