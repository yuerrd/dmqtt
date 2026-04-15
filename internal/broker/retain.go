package broker

import "sync"

// RetainStore stores the latest retained message per topic.
type RetainStore struct {
	mu       sync.RWMutex
	messages map[string]*retainedMessage
}

type retainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

func NewRetainStore() *RetainStore {
	return &RetainStore{
		messages: make(map[string]*retainedMessage),
	}
}

// Set stores or updates a retained message.
// An empty payload clears the retained message for that topic.
func (rs *RetainStore) Set(topic string, payload []byte, qos byte) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(payload) == 0 {
		delete(rs.messages, topic)
		return
	}

	rs.messages[topic] = &retainedMessage{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
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
