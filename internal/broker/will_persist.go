package broker

import (
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/langzp/dmqtt/internal/storage"
)

// WillStore manages will messages with in-memory cache and persistent storage.
type WillStore struct {
	mu    sync.RWMutex
	wills map[string]*WillMessage
	store storage.Store
}

func NewWillStore(store storage.Store) *WillStore {
	return &WillStore{
		wills: make(map[string]*WillMessage),
		store: store,
	}
}

// Load reads all persisted will messages from the store into memory.
func (ws *WillStore) Load() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	return ws.store.Scan([]byte("wm/"), func(key, value []byte) error {
		clientID := strings.TrimPrefix(string(key), "wm/")
		if strings.HasPrefix(clientID, "-published/") {
			return nil
		}
		var w WillMessage
		if err := json.Unmarshal(value, &w); err != nil {
			slog.Warn("skipping corrupt will message", "key", string(key), "error", err)
			return nil
		}
		w.ClientID = clientID
		ws.wills[clientID] = &w
		return nil
	})
}

func (ws *WillStore) Set(clientID string, will *WillMessage) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	will.ClientID = clientID
	ws.wills[clientID] = will

	data, err := json.Marshal(will)
	if err != nil {
		slog.Error("failed to marshal will", "client", clientID, "error", err)
		return
	}
	if err := ws.store.Set([]byte("wm/"+clientID), data); err != nil {
		slog.Error("failed to persist will", "client", clientID, "error", err)
	}
}

func (ws *WillStore) Get(clientID string) *WillMessage {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.wills[clientID]
}

func (ws *WillStore) Delete(clientID string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	delete(ws.wills, clientID)
	if err := ws.store.Delete([]byte("wm/" + clientID)); err != nil {
		slog.Error("failed to delete will", "client", clientID, "error", err)
	}
}

func (ws *WillStore) All() map[string]*WillMessage {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	result := make(map[string]*WillMessage, len(ws.wills))
	for k, v := range ws.wills {
		copy := *v
		result[k] = &copy
	}
	return result
}

func (ws *WillStore) MarkPublished(clientID string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	key := "wm-published/" + clientID
	if err := ws.store.Set([]byte(key), []byte("1")); err != nil {
		slog.Error("failed to mark will published", "client", clientID, "error", err)
	}
}

func (ws *WillStore) IsPublished(clientID string) bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	key := "wm-published/" + clientID
	val, err := ws.store.Get([]byte(key))
	return err == nil && len(val) > 0
}
