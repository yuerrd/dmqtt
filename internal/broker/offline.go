package broker

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/metrics"
)

const (
	PriorityHigh   byte = 0
	PriorityMedium byte = 1
	PriorityLow    byte = 2
)

type OfflineMessage struct {
	Topic     string
	Payload   []byte
	QoS       byte
	Priority  byte
	CreatedAt time.Time
	ExpiresAt time.Time
}

type OfflineStore struct {
	mu           sync.Mutex
	queues       map[string][]*OfflineMessage
	maxPerClient int
	defaultTTL   time.Duration
	tierConfig   *TierConfig
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
	}
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
	return result
}

func (s *OfflineStore) RemoveAll(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, clientID)
}

func (s *OfflineStore) Count(clientID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queues[clientID])
}
