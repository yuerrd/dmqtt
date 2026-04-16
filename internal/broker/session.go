package broker

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/langzp/dmqtt/internal/storage"
)

// Session stores per-client state.
type Session struct {
	ClientID      string          `json:"clientID"`
	CleanSession  bool            `json:"cleanSession"`
	Subscriptions map[string]byte `json:"subscriptions"`
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
			log.Printf("skipping corrupt session key=%s: %v", key, err)
			return nil
		}
		// Extract clientID from key "s/{clientID}"
		s.ClientID = strings.TrimPrefix(string(key), "s/")
		if s.Subscriptions == nil {
			s.Subscriptions = make(map[string]byte)
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

func (ss *SessionStore) Create(clientID string, cleanSession bool) *Session {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	s := &Session{
		ClientID:      clientID,
		CleanSession:  cleanSession,
		Subscriptions: make(map[string]byte),
	}
	ss.sessions[clientID] = s
	ss.persist(clientID, s)
	return s
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
		log.Printf("failed to persist session %s: %v", clientID, err)
		return
	}
	if err := ss.store.Set([]byte("s/"+clientID), data); err != nil {
		log.Printf("failed to write session %s: %v", clientID, err)
	}
}
