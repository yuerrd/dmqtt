package broker

import "sync"

// Session stores per-client state.
type Session struct {
	ClientID      string
	CleanSession  bool
	Subscriptions map[string]byte // filter -> QoS
}

// SessionStore manages client sessions.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewSessionStore() *SessionStore {
	return &SessionStore{
		sessions: make(map[string]*Session),
	}
}

func (ss *SessionStore) Get(clientID string) *Session {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.sessions[clientID]
}

func (ss *SessionStore) Create(clientID string, cleanSession bool) *Session {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	s := &Session{
		ClientID:      clientID,
		CleanSession:  cleanSession,
		Subscriptions: make(map[string]byte),
	}
	ss.sessions[clientID] = s
	return s
}

func (ss *SessionStore) Remove(clientID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.sessions, clientID)
}
