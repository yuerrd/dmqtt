package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/metrics"
	"github.com/langzp/dmqtt/internal/storage"
)

const (
	PriorityHigh   byte = 0
	PriorityMedium byte = 1
	PriorityLow    byte = 2
)

type OfflineMessage struct {
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	QoS       byte      `json:"qos"`
	Priority  byte      `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type OfflineStore struct {
	mu           sync.Mutex
	queues       map[string][]*OfflineMessage
	maxPerClient int
	defaultTTL   time.Duration
	tierConfig   *TierConfig
	store        storage.Store
	seqNums      map[string]uint64
}

type TierConfig struct {
	HighMax int
	HighTTL time.Duration
	MidMax  int
	MidTTL  time.Duration
	LowMax  int
	LowTTL  time.Duration
}

func NewOfflineStore(maxPerClient int, defaultTTL time.Duration) *OfflineStore {
	return &OfflineStore{
		queues:       make(map[string][]*OfflineMessage),
		maxPerClient: maxPerClient,
		defaultTTL:   defaultTTL,
		seqNums:      make(map[string]uint64),
	}
}

func (s *OfflineStore) SetStore(store storage.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

func (s *OfflineStore) LoadFromStore() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store == nil {
		return nil
	}

	// Load sequence numbers
	if err := s.store.Scan([]byte("om-seq/"), func(key, value []byte) error {
		clientID := strings.TrimPrefix(string(key), "om-seq/")
		if len(value) == 8 {
			s.seqNums[clientID] = binary.BigEndian.Uint64(value)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("loading sequence numbers: %w", err)
	}

	// Load offline messages
	return s.store.Scan([]byte("om/"), func(key, value []byte) error {
		keyStr := strings.TrimPrefix(string(key), "om/")
		parts := strings.SplitN(keyStr, "/", 2)
		if len(parts) != 2 {
			return nil
		}
		clientID := parts[0]

		var msg OfflineMessage
		if err := json.Unmarshal(value, &msg); err != nil {
			slog.Warn("skipping corrupt offline message", "key", string(key), "error", err)
			return nil
		}
		s.queues[clientID] = append(s.queues[clientID], &msg)
		return nil
	})
}

func (s *OfflineStore) SetTierConfig(cfg *TierConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tierConfig = cfg
}

func (s *OfflineStore) Enqueue(clientID string, msg *OfflineMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue := s.queues[clientID]
	now := time.Now()

	// Remove expired
	filtered := queue[:0]
	for _, m := range queue {
		if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
			continue
		}
		filtered = append(filtered, m)
	}
	queue = filtered

	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}

	// Apply tier-specific TTL
	if s.tierConfig != nil && msg.ExpiresAt.IsZero() {
		switch msg.Priority {
		case PriorityHigh:
			msg.ExpiresAt = msg.CreatedAt.Add(s.tierConfig.HighTTL)
		case PriorityMedium:
			msg.ExpiresAt = msg.CreatedAt.Add(s.tierConfig.MidTTL)
		case PriorityLow:
			msg.ExpiresAt = msg.CreatedAt.Add(s.tierConfig.LowTTL)
		}
	} else if msg.ExpiresAt.IsZero() && s.defaultTTL > 0 {
		msg.ExpiresAt = msg.CreatedAt.Add(s.defaultTTL)
	}

	// Check if queue is full
	if len(queue) >= s.maxPerClient {
		if s.tierConfig == nil {
			s.queues[clientID] = queue
			return fmt.Errorf("offline queue full for client %s (limit: %d)", clientID, s.maxPerClient)
		}
		// Try to evict lower priority (oldest first)
		evicted := false
		for p := int(PriorityLow); p >= int(msg.Priority); p-- {
			for i, m := range queue {
				if int(m.Priority) == p {
					priorityName := "low"
					if m.Priority == PriorityMedium {
						priorityName = "medium"
					} else if m.Priority == PriorityHigh {
						priorityName = "high"
					}
					metrics.OfflineMessageEvicted(priorityName)
					queue = append(queue[:i], queue[i+1:]...)
					evicted = true
					break
				}
			}
			if evicted {
				break
			}
		}
		if !evicted {
			s.queues[clientID] = queue
			return fmt.Errorf("offline queue full for client %s (limit: %d)", clientID, s.maxPerClient)
		}
	}

	s.queues[clientID] = append(queue, msg)
	s.clearPebbleMessages(clientID)
	return nil
}

func (s *OfflineStore) Dequeue(clientID string, maxCount int) []*OfflineMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[clientID]
	if !ok {
		return nil
	}

	now := time.Now()
	var valid []*OfflineMessage
	for _, m := range queue {
		if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
			continue
		}
		valid = append(valid, m)
	}

	sort.SliceStable(valid, func(i, j int) bool {
		if valid[i].Priority != valid[j].Priority {
			return valid[i].Priority < valid[j].Priority
		}
		return valid[i].CreatedAt.Before(valid[j].CreatedAt)
	})

	count := maxCount
	if count > len(valid) {
		count = len(valid)
	}
	result := valid[:count]
	remaining := valid[count:]

	if len(remaining) == 0 {
		delete(s.queues, clientID)
	} else {
		s.queues[clientID] = remaining
	}
	s.clearPebbleMessages(clientID)
	return result
}

func (s *OfflineStore) RemoveAll(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, clientID)
	s.removeAllPebble(clientID)
}

func (s *OfflineStore) persistMessage(clientID string, msg *OfflineMessage) {
	if s.store == nil {
		return
	}
	seq := s.seqNums[clientID]
	s.seqNums[clientID] = seq + 1

	key := fmt.Sprintf("om/%s/%020d", clientID, seq)
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal offline message", "client", clientID, "error", err)
		return
	}
	if err := s.store.Set([]byte(key), data); err != nil {
		slog.Error("failed to persist offline message", "client", clientID, "error", err)
		return
	}

	seqKey := fmt.Sprintf("om-seq/%s", clientID)
	seqVal := make([]byte, 8)
	binary.BigEndian.PutUint64(seqVal, seq+1)
	if err := s.store.Set([]byte(seqKey), seqVal); err != nil {
		slog.Error("failed to persist sequence number", "client", clientID, "error", err)
	}
}

func (s *OfflineStore) clearPebbleMessages(clientID string) {
	if s.store == nil {
		return
	}
	queue := s.queues[clientID]

	// Delete all existing messages for this client
	prefix := fmt.Sprintf("om/%s/", clientID)
	var keysToDelete [][]byte
	if err := s.store.Scan([]byte(prefix), func(key, _ []byte) error {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keysToDelete = append(keysToDelete, keyCopy)
		return nil
	}); err != nil {
		slog.Error("failed to scan offline messages for clearing", "client", clientID, "error", err)
	}
	for _, key := range keysToDelete {
		if err := s.store.Delete(key); err != nil {
			slog.Error("failed to delete offline message", "client", clientID, "key", string(key), "error", err)
		}
	}

	// Rewrite remaining
	s.seqNums[clientID] = 0
	for _, msg := range queue {
		s.persistMessage(clientID, msg)
	}

	if len(queue) == 0 {
		if err := s.store.Delete([]byte("om-seq/" + clientID)); err != nil {
			slog.Error("failed to delete sequence number", "client", clientID, "error", err)
		}
		delete(s.seqNums, clientID)
	}
}

func (s *OfflineStore) removeAllPebble(clientID string) {
	if s.store == nil {
		return
	}
	prefix := fmt.Sprintf("om/%s/", clientID)
	var keysToDelete [][]byte
	if err := s.store.Scan([]byte(prefix), func(key, _ []byte) error {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keysToDelete = append(keysToDelete, keyCopy)
		return nil
	}); err != nil {
		slog.Error("failed to scan offline messages for removal", "client", clientID, "error", err)
	}
	for _, key := range keysToDelete {
		if err := s.store.Delete(key); err != nil {
			slog.Error("failed to delete offline message", "client", clientID, "key", string(key), "error", err)
		}
	}
	if err := s.store.Delete([]byte("om-seq/" + clientID)); err != nil {
		slog.Error("failed to delete sequence number", "client", clientID, "error", err)
	}
	delete(s.seqNums, clientID)
}

func (s *OfflineStore) Count(clientID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queues[clientID])
}
