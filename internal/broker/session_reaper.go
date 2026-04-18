package broker

import (
	"log/slog"
	"time"
)

type SessionReaper struct {
	sessions *SessionStore
	subs     *SubscriptionIndex
	offline  *OfflineStore
	interval time.Duration
	done     chan struct{}
}

func NewSessionReaper(sessions *SessionStore, subs *SubscriptionIndex, offline *OfflineStore, interval time.Duration) *SessionReaper {
	return &SessionReaper{
		sessions: sessions,
		subs:     subs,
		offline:  offline,
		interval: interval,
		done:     make(chan struct{}),
	}
}

func (r *SessionReaper) Start() { go r.loop() }

func (r *SessionReaper) Stop() { close(r.done) }

func (r *SessionReaper) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.sweep()
		}
	}
}

func (r *SessionReaper) sweep() {
	expired := r.sessions.ExpiredSessions()
	for _, clientID := range expired {
		filters := r.subs.ClientFilters(clientID)
		for _, filter := range filters {
			r.subs.Remove(clientID, filter)
		}
		r.offline.RemoveAll(clientID)
		r.sessions.Remove(clientID)
		slog.Info("session expired", "clientID", clientID)
	}
}
