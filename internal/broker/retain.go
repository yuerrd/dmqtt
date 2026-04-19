package broker

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/yuerrd/dmqtt/internal/storage"
)

type retainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

type retainedJSON struct {
	Payload string `json:"payload"` // base64-encoded
	QoS     byte   `json:"qos"`
}

// RetainStore stores the latest retained message per topic.
type RetainStore struct {
	mu       sync.RWMutex
	messages map[string]*retainedMessage
	store    storage.Store
}

func NewRetainStore(store storage.Store) *RetainStore {
	return &RetainStore{
		messages: make(map[string]*retainedMessage),
		store:    store,
	}
}

// Load restores retained messages from persistent storage.
func (rs *RetainStore) Load() error {
	if rs.store == nil {
		return nil
	}
	return rs.store.Scan([]byte("r/"), func(key, value []byte) error {
		var rj retainedJSON
		if err := json.Unmarshal(value, &rj); err != nil {
			slog.Warn("skipping corrupt retained message", "key", string(key), "error", err)
			return nil
		}
		topic := strings.TrimPrefix(string(key), "r/")
		payload, err := base64.StdEncoding.DecodeString(rj.Payload)
		if err != nil {
			slog.Warn("skipping corrupt retained payload", "key", string(key), "error", err)
			return nil
		}
		rs.messages[topic] = &retainedMessage{
			Topic:   topic,
			Payload: payload,
			QoS:     rj.QoS,
		}
		return nil
	})
}

// Set stores or updates a retained message.
// An empty payload clears the retained message for that topic.
func (rs *RetainStore) Set(topic string, payload []byte, qos byte) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(payload) == 0 {
		delete(rs.messages, topic)
		if rs.store != nil {
			rs.store.Delete([]byte("r/" + topic))
		}
		return
	}

	rs.messages[topic] = &retainedMessage{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
	}

	if rs.store != nil {
		data, _ := json.Marshal(retainedJSON{
			Payload: base64.StdEncoding.EncodeToString(payload),
			QoS:     qos,
		})
		rs.store.Set([]byte("r/"+topic), data)
	}
}

// Match returns all retained messages matching the given filter.
func (rs *RetainStore) Match(filter string) []retainedMessage {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var result []retainedMessage
	for topic, msg := range rs.messages {
		if TopicMatch(filter, topic) {
			result = append(result, *msg)
		}
	}
	return result
}

// Count returns the number of retained messages.
func (rs *RetainStore) Count() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.messages)
}
