package broker

import (
	"testing"
	"time"
)

func TestSessionCreate_NewSignature(t *testing.T) {
	ss := NewSessionStore(nil)
	s := ss.Create("c1", true, 0)
	if !s.CleanStart {
		t.Error("CleanStart = false, want true")
	}
	if s.ExpiryInterval != 0 {
		t.Errorf("ExpiryInterval = %d, want 0", s.ExpiryInterval)
	}
}

func TestSessionCreate_PersistentV31(t *testing.T) {
	ss := NewSessionStore(nil)
	s := ss.Create("c2", false, 0xFFFFFFFF)
	if s.CleanStart {
		t.Error("CleanStart = true, want false")
	}
	if s.ExpiryInterval != 0xFFFFFFFF {
		t.Errorf("ExpiryInterval = %d, want 0xFFFFFFFF", s.ExpiryInterval)
	}
}

func TestSessionMarkDisconnected(t *testing.T) {
	ss := NewSessionStore(nil)
	ss.Create("c1", false, 300)
	ss.MarkDisconnected("c1")
	s := ss.Get("c1")
	if s.DisconnectedAt == nil {
		t.Fatal("DisconnectedAt is nil after MarkDisconnected")
	}
}

func TestSessionReaper_ExpiresSession(t *testing.T) {
	ss := NewSessionStore(nil)
	subs := NewSubscriptionIndex()
	offline := NewOfflineStore(100, 24*time.Hour)

	ss.Create("expire-me", false, 1)
	subs.Add("expire-me", "test/#", 1)
	offline.Enqueue("expire-me", &OfflineMessage{Topic: "t", Payload: []byte("p"), QoS: 0})

	now := time.Now().Add(-2 * time.Second)
	s := ss.Get("expire-me")
	s.DisconnectedAt = &now
	ss.persist("expire-me", s)

	reaper := NewSessionReaper(ss, subs, offline, 50*time.Millisecond)
	reaper.Start()
	time.Sleep(200 * time.Millisecond)
	reaper.Stop()

	if ss.Get("expire-me") != nil {
		t.Error("expired session was not removed")
	}
	if matches := subs.Match("test/foo"); len(matches) != 0 {
		t.Error("subscriptions not cleaned up")
	}
}

func TestSessionReaper_SkipsNeverExpire(t *testing.T) {
	ss := NewSessionStore(nil)
	subs := NewSubscriptionIndex()
	offline := NewOfflineStore(100, 24*time.Hour)

	ss.Create("permanent", false, 0xFFFFFFFF)
	now := time.Now().Add(-24 * time.Hour)
	s := ss.Get("permanent")
	s.DisconnectedAt = &now

	reaper := NewSessionReaper(ss, subs, offline, 50*time.Millisecond)
	reaper.Start()
	time.Sleep(200 * time.Millisecond)
	reaper.Stop()

	if ss.Get("permanent") == nil {
		t.Error("never-expire session was incorrectly removed")
	}
}

func TestSessionReaper_SkipsOnline(t *testing.T) {
	ss := NewSessionStore(nil)
	subs := NewSubscriptionIndex()
	offline := NewOfflineStore(100, 24*time.Hour)

	ss.Create("online", false, 1)
	// DisconnectedAt is nil — session is online

	reaper := NewSessionReaper(ss, subs, offline, 50*time.Millisecond)
	reaper.Start()
	time.Sleep(200 * time.Millisecond)
	reaper.Stop()

	if ss.Get("online") == nil {
		t.Error("online session was incorrectly removed")
	}
}
