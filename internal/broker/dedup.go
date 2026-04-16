package broker

import (
	"fmt"
	"sync"
	"time"
)

type dedupEntry struct {
	ExpiresAt time.Time
}

type DedupStore struct {
	mu      sync.Mutex
	entries map[string]*dedupEntry
	ttl     time.Duration
}

func NewDedupStore(ttl time.Duration) *DedupStore {
	return &DedupStore{
		entries: make(map[string]*dedupEntry),
		ttl:     ttl,
	}
}

func dedupKey(clientID string, packetID uint16) string {
	return fmt.Sprintf("%s:%d", clientID, packetID)
}

func (d *DedupStore) IsDuplicate(clientID string, packetID uint16) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := dedupKey(clientID, packetID)
	entry, ok := d.entries[key]
	if !ok {
		return false
	}
	if time.Now().After(entry.ExpiresAt) {
		delete(d.entries, key)
		return false
	}
	return true
}

func (d *DedupStore) MarkReceived(clientID string, packetID uint16) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[dedupKey(clientID, packetID)] = &dedupEntry{
		ExpiresAt: time.Now().Add(d.ttl),
	}
}

func (d *DedupStore) Remove(clientID string, packetID uint16) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.entries, dedupKey(clientID, packetID))
}
