package broker

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/storage"
	"github.com/langzp/dmqtt/internal/transport"
)

// Broker is the MQTT message broker.
type Broker struct {
	addr          string
	listener      transport.Listener
	subscriptions *SubscriptionIndex
	sessions      *SessionStore
	retainStore   *RetainStore
	dedupStore    *DedupStore
	offlineStore  *OfflineStore
	store         storage.Store

	inflightLimit int

	mu      sync.RWMutex
	clients map[string]*Client

	done chan struct{}
}

func New(addr string, store storage.Store) *Broker {
	return &Broker{
		addr:          addr,
		subscriptions: NewSubscriptionIndex(),
		sessions:      NewSessionStore(store),
		retainStore:   NewRetainStore(store),
		dedupStore:    NewDedupStore(180 * time.Second),
		offlineStore:  NewOfflineStore(1000, 24*time.Hour),
		store:         store,
		inflightLimit: 20,
		clients:       make(map[string]*Client),
		done:          make(chan struct{}),
	}
}

func (b *Broker) loadFromStorage() error {
	if err := b.sessions.Load(); err != nil {
		return fmt.Errorf("loading sessions: %w", err)
	}
	if err := b.retainStore.Load(); err != nil {
		return fmt.Errorf("loading retained messages: %w", err)
	}
	// Restore subscriptions from loaded sessions
	b.sessions.mu.RLock()
	for clientID, session := range b.sessions.sessions {
		if !session.CleanSession {
			for filter, qos := range session.Subscriptions {
				b.subscriptions.Add(clientID, filter, qos)
			}
		}
	}
	b.sessions.mu.RUnlock()
	return nil
}

func (b *Broker) Start() error {
	if err := b.loadFromStorage(); err != nil {
		return err
	}

	l, err := transport.NewTCPListener(b.addr)
	if err != nil {
		return err
	}
	
	b.mu.Lock()
	b.listener = l
	b.mu.Unlock()
	
	log.Printf("DMQTT listening on %s", l.Addr())

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-b.done:
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		c := newClient(conn, b)
		go c.serve()
	}
}

func (b *Broker) Stop() {
	close(b.done)
	
	b.mu.Lock()
	listener := b.listener
	b.mu.Unlock()
	
	if listener != nil {
		listener.Close()
	}

	b.mu.RLock()
	for _, c := range b.clients {
		c.conn.Close()
	}
	b.mu.RUnlock()
}

func (b *Broker) Addr() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	if b.listener == nil {
		return ""
	}
	return b.listener.Addr()
}

func (b *Broker) registerClient(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[c.clientID] = c
}

func (b *Broker) unregisterClient(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.clients[c.clientID]; ok && existing == c {
		delete(b.clients, c.clientID)
	}
}

func (b *Broker) disconnectExisting(clientID string) {
	b.mu.Lock()
	existing, ok := b.clients[clientID]
	if ok {
		delete(b.clients, clientID)
	}
	b.mu.Unlock()

	if ok {
		existing.conn.Close()
	}
}

func (b *Broker) routeMessage(topic string, payload []byte, qos byte, retain bool) {
	matches := b.subscriptions.Match(topic)

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, match := range matches {
		effectiveQoS := qos
		if match.QoS < effectiveQoS {
			effectiveQoS = match.QoS
		}

		client, ok := b.clients[match.ClientID]
		if ok {
			client.deliverMessage(topic, payload, effectiveQoS)
		} else {
			session := b.sessions.Get(match.ClientID)
			if session != nil && !session.CleanSession {
				b.offlineStore.Enqueue(match.ClientID, &OfflineMessage{
					Topic:   topic,
					Payload: payload,
					QoS:     effectiveQoS,
				})
			}
		}
	}
}

func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}
