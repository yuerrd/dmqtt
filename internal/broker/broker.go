package broker

import (
	"log"
	"sync"

	"github.com/langzp/dmqtt/internal/codec"
	"github.com/langzp/dmqtt/internal/transport"
)

// Broker is the MQTT message broker.
type Broker struct {
	addr          string
	listener      transport.Listener
	subscriptions *SubscriptionIndex
	sessions      *SessionStore
	retainStore   *RetainStore

	mu      sync.RWMutex
	clients map[string]*Client

	done chan struct{}
}

func New(addr string) *Broker {
	return &Broker{
		addr:          addr,
		subscriptions: NewSubscriptionIndex(),
		sessions:      NewSessionStore(),
		retainStore:   NewRetainStore(),
		clients:       make(map[string]*Client),
		done:          make(chan struct{}),
	}
}

func (b *Broker) Start() error {
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
		client, ok := b.clients[match.ClientID]
		if !ok {
			continue
		}

		// Phase 1 simplified: forward all as QoS 0
		pubPkt := &codec.PublishPacket{
			Topic:   topic,
			QoS:     0,
			Retain:  false,
			Payload: payload,
		}

		go client.send(pubPkt.Encode())
	}
}

func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}
