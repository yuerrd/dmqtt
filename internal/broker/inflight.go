package broker

import (
	"sync"
	"time"
)

type InflightMessage struct {
	PacketID  uint16
	Topic     string
	Payload   []byte
	QoS       byte
	Timestamp time.Time
}

type InflightStore struct {
	mu       sync.Mutex
	messages map[uint16]*InflightMessage
	limit    int
}

func NewInflightStore(limit int) *InflightStore {
	return &InflightStore{
		messages: make(map[uint16]*InflightMessage),
		limit:    limit,
	}
}

func (s *InflightStore) Add(msg *InflightMessage) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.messages) >= s.limit {
		return false
	}
	s.messages[msg.PacketID] = msg
	return true
}

func (s *InflightStore) Get(packetID uint16) *InflightMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messages[packetID]
}

func (s *InflightStore) Remove(packetID uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.messages, packetID)
}

func (s *InflightStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}
