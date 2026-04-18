package broker

import (
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/storage"
)

// Session stores per-client state.
type Session struct {
	ClientID         string          `json:"clientID"`
	CleanStart       bool            `json:"cleanStart"`
	ExpiryInterval   uint32          `json:"expiryInterval"`
	Subscriptions    map[string]byte `json:"subscriptions"`
	DisconnectedAt   *time.Time      `json:"disconnectedAt,omitempty"`
	Epoch            uint64          `json:"epoch"`
	ConnectTimestamp int64           `json:"connectTimestamp"`
	NodeID           string          `json:"nodeID,omitempty"`
}

// SessionStore manages client sessions with optional persistence.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	store    storage.Store // nil = in-memory only
}

func NewSessionStore(store storage.Store) *SessionStore {
	return &SessionStore{
		sessions: make(map[string]*Session),
		store:    store,
	}
}

// Load restores all sessions from persistent storage into memory.
// Call once at broker startup.
func (ss *SessionStore) Load() error {
	if ss.store == nil {
		return nil
	}
	return ss.store.Scan([]byte("s/"), func(key, value []byte) error {
		var s Session
		if err := json.Unmarshal(value, &s); err != nil {
			slog.Warn("skipping corrupt session", "key", string(key), "error", err)
			return nil
		}
		s.ClientID = strings.TrimPrefix(string(key), "s/")
		if s.Subscriptions == nil {
			s.Subscriptions = make(map[string]byte)
		}
		// Backwards compat: detect old format with "cleanSession" field
		var raw map[string]interface{}
		if json.Unmarshal(value, &raw) == nil {
			if _, hasOld := raw["cleanSession"]; hasOld {
				if _, hasNew := raw["cleanStart"]; !hasNew {
					cs, _ := raw["cleanSession"].(bool)
					s.CleanStart = cs
					if cs {
						s.ExpiryInterval = 0
					} else {
						s.ExpiryInterval = 0xFFFFFFFF
					}
				}
			}
		}
		ss.sessions[s.ClientID] = &s
		return nil
	})
}

func (ss *SessionStore) Get(clientID string) *Session {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.sessions[clientID]
}

func (ss *SessionStore) Create(clientID string, cleanStart bool, expiryInterval uint32) *Session {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	s := &Session{
		ClientID:       clientID,
		CleanStart:     cleanStart,
		ExpiryInterval: expiryInterval,
		Subscriptions:  make(map[string]byte),
	}
	ss.sessions[clientID] = s
	ss.persist(clientID, s)
	return s
}

func (ss *SessionStore) MarkDisconnected(clientID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if s, ok := ss.sessions[clientID]; ok {
		now := time.Now()
		s.DisconnectedAt = &now
		ss.persist(clientID, s)
	}
}

func (ss *SessionStore) ExpiredSessions() []string {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	var expired []string
	now := time.Now()
	for id, s := range ss.sessions {
		if s.DisconnectedAt == nil {
			continue
		}
		if s.ExpiryInterval == 0xFFFFFFFF {
			continue
		}
		deadline := s.DisconnectedAt.Add(time.Duration(s.ExpiryInterval) * time.Second)
		if now.After(deadline) {
			expired = append(expired, id)
		}
	}
	return expired
}

func (ss *SessionStore) Remove(clientID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.sessions, clientID)
	if ss.store != nil {
		ss.store.Delete([]byte("s/" + clientID))
	}
}

// Save persists the current state of a session (call after subscription changes).
func (ss *SessionStore) Save(clientID string) {
	ss.mu.RLock()
	s, ok := ss.sessions[clientID]
	ss.mu.RUnlock()
	if ok {
		ss.persist(clientID, s)
	}
}

func (ss *SessionStore) persist(clientID string, s *Session) {
	if ss.store == nil {
		return
	}
	data, err := json.Marshal(s)
	if err != nil {
		slog.Error("failed to persist session", "client", clientID, "error", err)
		return
	}
	if err := ss.store.Set([]byte("s/"+clientID), data); err != nil {
		slog.Error("failed to write session", "client", clientID, "error", err)
	}
}
