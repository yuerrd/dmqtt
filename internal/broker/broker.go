package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/auth"
	"github.com/langzp/dmqtt/internal/circuitbreaker"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/langzp/dmqtt/internal/metrics"
	"github.com/langzp/dmqtt/internal/plugin"
	"github.com/langzp/dmqtt/internal/ratelimit"
	"github.com/langzp/dmqtt/internal/storage"
	"github.com/langzp/dmqtt/internal/transport"
)

// Broker is the MQTT message broker.
type Broker struct {
	addr          string
	listeners     []transport.Listener
	subscriptions *SubscriptionIndex
	sessions      *SessionStore
	retainStore   *RetainStore
	dedupStore    *DedupStore
	offlineStore  *OfflineStore
	store         storage.Store
	cluster       *cluster.Cluster
	authenticator auth.Authenticator
	authorizer    auth.Authorizer
	rateLimiter   ratelimit.RateLimiter
	interceptors  *plugin.InterceptorChain

	inflightLimit int

	mu      sync.RWMutex
	clients map[string]*Client

	done   chan struct{}
	reaper *SessionReaper
}

func New(addr string, store storage.Store) *Broker {
	noop := &auth.NoopAuth{}
	return &Broker{
		addr:          addr,
		subscriptions: NewSubscriptionIndex(),
		sessions:      NewSessionStore(store),
		retainStore:   NewRetainStore(store),
		dedupStore:    NewDedupStore(180 * time.Second),
		offlineStore:  NewOfflineStore(1000, 24*time.Hour),
		store:         store,
		authenticator: noop,
		authorizer:    noop,
		rateLimiter:   &ratelimit.NoopRateLimiter{},
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
		if session.ExpiryInterval != 0 {
			for filter, qos := range session.Subscriptions {
				b.subscriptions.Add(clientID, filter, qos)
			}
		}
	}
	b.sessions.mu.RUnlock()
	return nil
}

// Serve accepts connections from the given listeners.
// Blocks until Stop() is called.
func (b *Broker) Serve(listeners ...transport.Listener) error {
	if err := b.loadFromStorage(); err != nil {
		return err
	}

	b.mu.Lock()
	b.listeners = listeners
	b.mu.Unlock()

	b.reaper = NewSessionReaper(b.sessions, b.subscriptions, b.offlineStore, 60*time.Second)
	b.reaper.Start()

	for _, ln := range listeners {
		slog.Info("DMQTT listening", "addr", ln.Addr())
	}

	for _, ln := range listeners {
		go b.acceptLoop(ln)
	}

	// Wait until Stop() is called
	<-b.done
	return nil
}

func (b *Broker) acceptLoop(l transport.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-b.done:
				return
			default:
				slog.Error("accept error", "error", err)
				continue
			}
		}
		if err := b.rateLimiter.AllowConnect(""); err != nil {
			slog.Warn("connection rate limited", "error", err)
			metrics.RateLimitRejected("connect", err.Error())
			conn.Close()
			continue
		}
		c := newClient(conn, b)
		go c.serve()
	}
}

func (b *Broker) Start() error {
	l, err := transport.NewTCPListener(b.addr)
	if err != nil {
		return err
	}
	return b.Serve(l)
}

func (b *Broker) Stop() {
	close(b.done)

	if b.reaper != nil {
		b.reaper.Stop()
	}

	b.mu.Lock()
	listeners := b.listeners
	b.mu.Unlock()

	for _, l := range listeners {
		l.Close()
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
	if len(b.listeners) == 0 {
		return ""
	}
	return b.listeners[0].Addr()
}

// SetCluster attaches a cluster to the broker and registers handlers.
// Must be called before Start().
func (b *Broker) SetCluster(c *cluster.Cluster) {
	b.cluster = c
	if c != nil {
		c.SetForwardHandler(func(msg cluster.ForwardMessage) {
			b.routeMessage(msg.Topic, msg.Payload, msg.QoS, msg.Retain, true)
		})
		c.SetLocalFiltersProvider(func() []string {
			return b.subscriptions.AllFilters()
		})
		c.SetRemoteConnectHandler(func(deviceID, nodeID string) {
			b.disconnectExisting(deviceID)
		})
		c.SetLocalDevicesProvider(func() []string {
			return b.ConnectedClientIDs()
		})
	}
}

// SetAuth sets the authentication and authorization providers.
// Must be called before Start(). If not called, NoopAuth is used.
func (b *Broker) SetAuth(authn auth.Authenticator, authz auth.Authorizer) {
	b.authenticator = authn
	b.authorizer = authz
}

// SetRateLimiter sets the rate limiter. Must be called before Start().
func (b *Broker) SetRateLimiter(rl ratelimit.RateLimiter) {
	b.rateLimiter = rl
}

// SetInterceptors sets the interceptor chain for the broker.
func (b *Broker) SetInterceptors(chain *plugin.InterceptorChain) {
	b.interceptors = chain
}

// Cluster returns the attached cluster, or nil if standalone.
func (b *Broker) Cluster() *cluster.Cluster {
	return b.cluster
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
		existing.sendDisconnectAndClose(0x8E) // DisconnSessionTakenOver
	}
}

// RouteMessage publishes a message into the broker's routing pipeline.
// Used by rule engine publish actions to republish messages.
func (b *Broker) RouteMessage(topic string, payload []byte, qos byte) {
	b.routeMessage(topic, payload, qos, false, false)
}

func (b *Broker) routeMessage(topic string, payload []byte, qos byte, retain bool, forwarded bool) {
	matches := b.subscriptions.Match(topic)

	b.mu.RLock()
	for _, match := range matches {
		effectiveQoS := qos
		if match.QoS < effectiveQoS {
			effectiveQoS = match.QoS
		}

		client, ok := b.clients[match.ClientID]
		if ok {
			if b.interceptors != nil {
				evt := &plugin.DeliveryEvent{
					ClientID: match.ClientID,
					Topic:    topic,
					Payload:  payload,
					QoS:      effectiveQoS,
				}
				if err := b.interceptors.OnDelivery(context.Background(), evt); err != nil {
					continue
				}
				client.deliverMessage(evt.Topic, evt.Payload, evt.QoS)
			} else {
				client.deliverMessage(topic, payload, effectiveQoS)
			}
		} else {
			session := b.sessions.Get(match.ClientID)
			if session != nil && session.ExpiryInterval != 0 {
				b.offlineStore.Enqueue(match.ClientID, &OfflineMessage{
					Topic:   topic,
					Payload: payload,
					QoS:     effectiveQoS,
				})
			}
		}
	}
	b.mu.RUnlock()

	// Remote forwarding (only if not already forwarded and cluster is active)
	if !forwarded && b.cluster != nil {
		remoteMatches := b.cluster.RemoteSubs().Match(topic)
		for _, rm := range remoteMatches {
			effectiveQoS := qos
			if rm.MaxQoS < effectiveQoS {
				effectiveQoS = rm.MaxQoS
			}
			err := b.cluster.Forward(rm.NodeID, cluster.ForwardMessage{
				Topic:   topic,
				Payload: payload,
				QoS:     effectiveQoS,
				Retain:  retain,
			})
			if err != nil {
				if err == circuitbreaker.ErrCircuitOpen {
					slog.Warn("circuit breaker open, storing offline", "peer", rm.NodeID, "topic", topic)
					// Store for all subscribers on that remote node — we can't forward
					// The message will be delivered when the device reconnects (possibly to us)
				} else {
					slog.Error("forward failed", "peer", rm.NodeID, "error", err)
				}
				continue
			}
			metrics.MessageForwarded()
		}
	}
}

func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// ConnectedClientIDs returns all currently connected client IDs.
func (b *Broker) ConnectedClientIDs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ids := make([]string, 0, len(b.clients))
	for id := range b.clients {
		ids = append(ids, id)
	}
	return ids
}

// GetClientInfo returns metadata for a connected client, or nil if not found.
func (b *Broker) GetClientInfo(clientID string) *ClientInfo {
	b.mu.RLock()
	c, ok := b.clients[clientID]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return &ClientInfo{
		ClientID:        c.clientID,
		Username:        c.username,
		RemoteAddr:      c.conn.RemoteAddr().String(),
		ProtocolVersion: c.protocolVersion,
		ConnectedAt:     c.connectedAt,
		KeepAlive:       c.keepAlive,
	}
}

// ActiveSubscriptions returns the total number of active subscriptions.
func (b *Broker) ActiveSubscriptions() int {
	return b.subscriptions.Count()
}

// RetainedMessageCount returns the number of retained messages.
func (b *Broker) RetainedMessageCount() int {
	return b.retainStore.Count()
}

// ClusterNodeCount returns the number of cluster nodes, or 0 if standalone.
func (b *Broker) ClusterNodeCount() int {
	if b.cluster != nil {
		return b.cluster.Size()
	}
	return 0
}

// IsReady returns true if the broker is accepting connections.
func (b *Broker) IsReady() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.listeners) > 0
}

// --- MigrationBrokerAPI implementation ---

// DisconnectDevice forcefully disconnects a device (same as disconnectExisting).
func (b *Broker) DisconnectDevice(deviceID string) {
	b.disconnectExisting(deviceID)
}

// GetOfflineMessages returns offline messages for a device as migration-friendly format.
func (b *Broker) GetOfflineMessages(deviceID string) []cluster.MigrateOfflineMsg {
	msgs := b.offlineStore.Dequeue(deviceID, b.offlineStore.Count(deviceID))
	result := make([]cluster.MigrateOfflineMsg, len(msgs))
	for i, m := range msgs {
		result[i] = cluster.MigrateOfflineMsg{
			Topic:   m.Topic,
			Payload: m.Payload,
			QoS:     m.QoS,
		}
	}
	// Re-enqueue since Dequeue is destructive — we want to keep until cleanup
	for _, m := range msgs {
		b.offlineStore.Enqueue(deviceID, m)
	}
	return result
}

// DeleteOfflineMessages removes all offline messages for a device.
func (b *Broker) DeleteOfflineMessages(deviceID string) {
	b.offlineStore.RemoveAll(deviceID)
}

// GetSessionData returns session data for migration.
func (b *Broker) GetSessionData(clientID string) *cluster.MigrateSessionData {
	session := b.sessions.Get(clientID)
	if session == nil {
		return nil
	}
	subs := make(map[string]byte, len(session.Subscriptions))
	for k, v := range session.Subscriptions {
		subs[k] = v
	}
	return &cluster.MigrateSessionData{
		ClientID:       session.ClientID,
		CleanStart:     session.CleanStart,
		ExpiryInterval: session.ExpiryInterval,
		Subscriptions:  subs,
	}
}

// DeleteSession removes a session.
func (b *Broker) DeleteSession(clientID string) {
	b.sessions.Remove(clientID)
}

// ImportMigrateData stores incoming migration data.
func (b *Broker) ImportMigrateData(msg cluster.MigrateDataMessage) {
	// Import offline messages
	for _, m := range msg.Messages {
		b.offlineStore.Enqueue(msg.DeviceID, &OfflineMessage{
			Topic:   m.Topic,
			Payload: m.Payload,
			QoS:     m.QoS,
		})
	}
	// Import session
	if msg.Session != nil {
		session := b.sessions.Create(msg.Session.ClientID, msg.Session.CleanStart, msg.Session.ExpiryInterval)
		for filter, qos := range msg.Session.Subscriptions {
			session.Subscriptions[filter] = qos
			b.subscriptions.Add(msg.Session.ClientID, filter, qos)
		}
		b.sessions.Save(msg.Session.ClientID)
	}
}
