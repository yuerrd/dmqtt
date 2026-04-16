package broker

import "sync"

// SubscriptionMatch represents a matched subscriber for a topic.
type SubscriptionMatch struct {
	ClientID string
	QoS      byte
}

type subscriptionEntry struct {
	ClientID string
	QoS      byte
}

// SubscriptionIndex is a thread-safe in-memory subscription store.
type SubscriptionIndex struct {
	mu      sync.RWMutex
	filters map[string][]subscriptionEntry
	clients map[string]map[string]struct{}
}

func NewSubscriptionIndex() *SubscriptionIndex {
	return &SubscriptionIndex{
		filters: make(map[string][]subscriptionEntry),
		clients: make(map[string]map[string]struct{}),
	}
}

func (idx *SubscriptionIndex) Add(clientID, filter string, qos byte) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entries := idx.filters[filter]
	for i, e := range entries {
		if e.ClientID == clientID {
			entries[i].QoS = qos
			return
		}
	}

	idx.filters[filter] = append(entries, subscriptionEntry{ClientID: clientID, QoS: qos})

	if idx.clients[clientID] == nil {
		idx.clients[clientID] = make(map[string]struct{})
	}
	idx.clients[clientID][filter] = struct{}{}
}

func (idx *SubscriptionIndex) Remove(clientID, filter string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entries := idx.filters[filter]
	for i, e := range entries {
		if e.ClientID == clientID {
			idx.filters[filter] = append(entries[:i], entries[i+1:]...)
			break
		}
	}

	if len(idx.filters[filter]) == 0 {
		delete(idx.filters, filter)
	}

	if clientFilters, ok := idx.clients[clientID]; ok {
		delete(clientFilters, filter)
		if len(clientFilters) == 0 {
			delete(idx.clients, clientID)
		}
	}
}

func (idx *SubscriptionIndex) RemoveAll(clientID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	filters, ok := idx.clients[clientID]
	if !ok {
		return
	}

	for filter := range filters {
		entries := idx.filters[filter]
		for i, e := range entries {
			if e.ClientID == clientID {
				idx.filters[filter] = append(entries[:i], entries[i+1:]...)
				break
			}
		}
		if len(idx.filters[filter]) == 0 {
			delete(idx.filters, filter)
		}
	}

	delete(idx.clients, clientID)
}

// AllFilters returns all unique topic filters with active subscriptions.
// Used to re-broadcast subscriptions when a new node joins the cluster.
func (idx *SubscriptionIndex) AllFilters() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	filters := make([]string, 0, len(idx.filters))
	for filter := range idx.filters {
		filters = append(filters, filter)
	}
	return filters
}

func (idx *SubscriptionIndex) Match(topic string) []SubscriptionMatch {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	best := make(map[string]byte)

	for filter, entries := range idx.filters {
		if TopicMatch(filter, topic) {
			for _, e := range entries {
				if existing, ok := best[e.ClientID]; !ok || e.QoS > existing {
					best[e.ClientID] = e.QoS
				}
			}
		}
	}

	result := make([]SubscriptionMatch, 0, len(best))
	for clientID, qos := range best {
		result = append(result, SubscriptionMatch{ClientID: clientID, QoS: qos})
	}
	return result
}
