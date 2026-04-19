package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
)

// ClusterConfig holds cluster configuration.
type ClusterConfig struct {
	Enabled       bool
	Name          string
	NodeID        string
	Host          string
	GossipPort    int
	TransportPort int
	MQTTPort      int
	HTTPPort      int
	Region        string
	Seeds         []string
	VirtualNodes  int
	ReplicaCount  int
}

// DefaultClusterConfig returns sensible defaults.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		Enabled:       false,
		Name:          "dmqtt",
		GossipPort:    7000,
		TransportPort: 8000,
		MQTTPort:      1883,
		VirtualNodes:  150,
		ReplicaCount:  3,
	}
}

// Cluster coordinates membership and consistent hashing.
type Cluster struct {
	membership *Membership
	ring       *Ring
	self       NodeInfo
	config     ClusterConfig

	transport            *PeerTransport
	remoteSubs           *RemoteSubIndex
	connections          *ConnectionIndex
	broadcasts           *memberlist.TransmitLimitedQueue
	forwardHandler       func(ForwardMessage)
	localFiltersProvider func() []string
	localDevicesProvider func() []string
	onRemoteConnect      atomic.Value // stores func(deviceID, nodeID string)

	replicator      *Replicator
	takeoverManager *TakeoverManager
	willPublisher   func(clientID string)

	coordinator     *MigrationCoordinator
	executor        *MigrationExecutor
	brokerAPI       MigrationBrokerAPI
	autoRebalance   bool
	migrationConfig MigrationConfig

	done chan struct{}
}

// NewCluster creates a new Cluster from the given config.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("cluster node ID is required")
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.GossipPort == 0 {
		cfg.GossipPort = 7000
	}
	if cfg.TransportPort == 0 {
		cfg.TransportPort = cfg.GossipPort + 1000
	}
	if cfg.MQTTPort == 0 {
		cfg.MQTTPort = 1883
	}
	if cfg.VirtualNodes == 0 {
		cfg.VirtualNodes = 150
	}
	if cfg.ReplicaCount == 0 {
		cfg.ReplicaCount = 3
	}

	self := NodeInfo{
		ID:            cfg.NodeID,
		Host:          cfg.Host,
		GossipPort:    cfg.GossipPort,
		TransportPort: cfg.TransportPort,
		MQTTPort:      cfg.MQTTPort,
		HTTPPort:      cfg.HTTPPort,
		Region:        cfg.Region,
	}

	ring := NewRing(cfg.VirtualNodes)
	remoteSubs := NewRemoteSubIndex()
	connections := NewConnectionIndex()

	c := &Cluster{
		ring:        ring,
		self:        self,
		config:      cfg,
		remoteSubs:  remoteSubs,
		connections: connections,
		done:        make(chan struct{}),
	}

	c.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return c.Size() },
		RetransmitMult: 3,
	}

	membership, err := NewMembership(self, cfg.Seeds, c.broadcasts, c.handleBroadcastMsg)
	if err != nil {
		return nil, fmt.Errorf("creating membership: %w", err)
	}
	c.membership = membership

	// Initialize ring with current members
	ring.Update(membership.Members())

	transport, err := NewPeerTransport(
		fmt.Sprintf("%s:%d", cfg.Host, cfg.TransportPort),
		cfg.NodeID,
		func(msg ForwardMessage) {
			if c.forwardHandler != nil {
				c.forwardHandler(msg)
			}
		},
	)
	if err != nil {
		membership.Leave(5 * time.Second)
		return nil, fmt.Errorf("creating peer transport: %w", err)
	}
	c.transport = transport

	go c.eventLoop()

	return c, nil
}

// eventLoop watches for membership changes and updates the ring.
func (c *Cluster) eventLoop() {
	for {
		select {
		case ev := <-c.membership.Events():
			// Snapshot ring before update for diff calculation
			var oldRing *Ring
			if c.autoRebalance && c.coordinator != nil {
				oldRing = c.ring.Snapshot()
			}

			switch ev.Type {
			case NodeJoin:
				slog.Info("cluster: node joined", "node", ev.Node.ID)
				if ev.Node.ID != c.self.ID {
					c.transport.AddPeer(ev.Node.ID, fmt.Sprintf("%s:%d", ev.Node.Host, ev.Node.TransportPort))
					if c.localFiltersProvider != nil {
						go c.rebroadcastLocalSubs(c.localFiltersProvider())
					}
					if c.localDevicesProvider != nil {
						go c.rebroadcastLocalConnections(c.localDevicesProvider())
					}
				}
			case NodeLeave:
				slog.Info("cluster: node left", "node", ev.Node.ID)
				c.transport.RemovePeer(ev.Node.ID)

				// Publish will messages for devices that were on the dead node
				if c.willPublisher != nil {
					deadNodeDevices := c.connections.NodeDevices(ev.Node.ID)
					go c.publishWillsForDeadNode(deadNodeDevices)
				}

				c.remoteSubs.RemoveNode(ev.Node.ID)
				c.connections.RemoveNode(ev.Node.ID)
			case NodeUpdate:
				slog.Info("cluster: node updated", "node", ev.Node.ID)
			}
			c.ring.Update(c.membership.Members())

			// Auto-rebalance: compute diff and create migrations
			if oldRing != nil && c.localDevicesProvider != nil {
				go c.autoRebalanceAfterRingChange(oldRing)
			}
		case <-c.done:
			return
		}
	}
}

func (c *Cluster) autoRebalanceAfterRingChange(oldRing *Ring) {
	localDevices := c.localDevicesProvider()
	if len(localDevices) == 0 {
		return
	}

	diff := DiffOwnership(oldRing, c.ring, localDevices)
	myDiff, ok := diff[c.self.ID]
	if !ok {
		return
	}

	for toNode, deviceIDs := range myDiff {
		if !c.coordinator.CanStartMore() {
			slog.Warn("auto-rebalance: max parallel migrations reached, skipping", "to", toNode, "devices", len(deviceIDs))
			continue
		}

		cfg := c.migrationConfig
		if cfg.BatchSize == 0 {
			cfg = DefaultMigrationConfig()
		}

		id := fmt.Sprintf("auto-%s-%s-%d", c.self.ID, toNode, time.Now().UnixMilli())
		m := NewShardMigration(id, c.self.ID, toNode, deviceIDs, cfg)
		c.coordinator.Submit(m)

		go func(migration *ShardMigration) {
			if err := c.executor.Execute(migration); err != nil {
				slog.Error("auto-rebalance migration failed", "id", migration.ID, "error", err)
			}
		}(m)

		slog.Info("auto-rebalance: migration created", "id", id, "to", toNode, "devices", len(deviceIDs))
	}
}

// SetMigrationBrokerAPI sets the broker API for migrations.
func (c *Cluster) SetMigrationBrokerAPI(api MigrationBrokerAPI) {
	c.brokerAPI = api
	c.coordinator = NewMigrationCoordinator(3)
	c.executor = NewMigrationExecutor(c.transport, c.connections, api)
}

// SetAutoRebalance enables/disables automatic migration on ring changes.
func (c *Cluster) SetAutoRebalance(enabled bool) {
	c.autoRebalance = enabled
}

// SetMigrationConfig sets the default config for auto-triggered migrations.
func (c *Cluster) SetMigrationConfig(cfg MigrationConfig) {
	c.migrationConfig = cfg
}

// Coordinator returns the migration coordinator.
func (c *Cluster) Coordinator() *MigrationCoordinator {
	return c.coordinator
}

// TriggerMigration manually creates and executes a migration.
func (c *Cluster) TriggerMigration(toNode string, deviceIDs []string) (*ShardMigration, error) {
	if c.coordinator == nil {
		return nil, fmt.Errorf("migration not configured: call SetMigrationBrokerAPI first")
	}
	if !c.coordinator.CanStartMore() {
		return nil, fmt.Errorf("max parallel migrations reached")
	}

	cfg := c.migrationConfig
	if cfg.BatchSize == 0 {
		cfg = DefaultMigrationConfig()
	}

	id := fmt.Sprintf("mig-%s-%s-%d", c.self.ID, toNode, time.Now().UnixMilli())
	m := NewShardMigration(id, c.self.ID, toNode, deviceIDs, cfg)
	c.coordinator.Submit(m)

	go func() {
		if err := c.executor.Execute(m); err != nil {
			slog.Error("migration failed", "id", m.ID, "error", err)
		}
	}()

	return m, nil
}

// Stop gracefully leaves the cluster and shuts down.
func (c *Cluster) Stop() error {
	if c.transport != nil {
		c.transport.Stop()
	}
	close(c.done)
	return c.membership.Leave(5 * time.Second)
}

// RemoteSubs returns the remote subscription index.
func (c *Cluster) RemoteSubs() *RemoteSubIndex { return c.remoteSubs }

// SetForwardHandler sets the callback invoked when a forwarded message arrives.
func (c *Cluster) SetForwardHandler(fn func(ForwardMessage)) { c.forwardHandler = fn }

// SetLocalFiltersProvider sets a function that returns current local subscription filters.
func (c *Cluster) SetLocalFiltersProvider(fn func() []string) { c.localFiltersProvider = fn }

// Forward sends a message to the specified remote node.
func (c *Cluster) Forward(nodeID string, msg ForwardMessage) error {
	msg.Type = MsgForward
	msg.Forwarded = true
	if msg.ID == 0 {
		msg.ID = c.transport.NextID()
	}
	if msg.QoS == 0 {
		return c.transport.Send(nodeID, msg)
	}
	return c.transport.SendReliable(nodeID, msg, 3, 5*time.Second)
}

// BroadcastSubscribe queues a subscription broadcast via gossip.
func (c *Cluster) BroadcastSubscribe(topicFilter string, qos byte) {
	msg := SubBroadcast{Type: "sub", NodeID: c.self.ID, TopicFilter: topicFilter, QoS: qos}
	c.broadcasts.QueueBroadcast(&subBroadcastItem{msg: msg})
}

// BroadcastUnsubscribe queues an unsubscription broadcast via gossip.
func (c *Cluster) BroadcastUnsubscribe(topicFilter string) {
	msg := SubBroadcast{Type: "unsub", NodeID: c.self.ID, TopicFilter: topicFilter}
	c.broadcasts.QueueBroadcast(&subBroadcastItem{msg: msg})
}

func (c *Cluster) handleBroadcastMsg(data []byte) {
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &peek); err != nil {
		slog.Error("cluster: unmarshal broadcast peek", "error", err)
		return
	}

	switch peek.Type {
	case "sub", "unsub":
		var msg SubBroadcast
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Error("cluster: unmarshal sub broadcast", "error", err)
			return
		}
		if msg.NodeID == c.self.ID {
			return
		}
		HandleSubBroadcast(c.remoteSubs, msg)
	case "conn", "disconn":
		var msg ConnBroadcast
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Error("cluster: unmarshal conn broadcast", "error", err)
			return
		}
		if msg.NodeID == c.self.ID {
			return
		}
		HandleConnBroadcast(c.connections, msg)
		if msg.Type == "conn" {
			if fn, ok := c.onRemoteConnect.Load().(func(string, string)); ok && fn != nil {
				fn(msg.DeviceID, msg.NodeID)
			}
		}
	default:
		slog.Warn("cluster: unknown broadcast type", "type", peek.Type)
	}
}

func (c *Cluster) rebroadcastLocalSubs(localFilters []string) {
	for _, filter := range localFilters {
		c.BroadcastSubscribe(filter, 2)
	}
}

func (c *Cluster) rebroadcastLocalConnections(deviceIDs []string) {
	for _, deviceID := range deviceIDs {
		c.BroadcastConnect(deviceID)
	}
}

// Self returns this node's info.
func (c *Cluster) Self() NodeInfo {
	return c.self
}

// Members returns all live cluster members.
func (c *Cluster) Members() []NodeInfo {
	return c.membership.Members()
}

// LocateDevice returns the primary node responsible for a device.
func (c *Cluster) LocateDevice(deviceID string) (NodeInfo, bool) {
	return c.ring.LocateDevice(deviceID)
}

// LocateDeviceN returns up to n replica nodes for a device.
func (c *Cluster) LocateDeviceN(deviceID string, n int) []NodeInfo {
	if n <= 0 {
		n = c.config.ReplicaCount
	}
	return c.ring.LocateDeviceN(deviceID, n)
}

// IsLocal returns true if this node is the primary owner of the device.
func (c *Cluster) IsLocal(deviceID string) bool {
	return c.ring.IsLocal(deviceID, c.self.ID)
}

// Size returns the number of nodes in the cluster.
func (c *Cluster) Size() int {
	return c.ring.Size()
}

// Connections returns the connection index.
func (c *Cluster) Connections() *ConnectionIndex { return c.connections }

// BroadcastConnect queues a connect broadcast via gossip.
func (c *Cluster) BroadcastConnect(deviceID string) {
	msg := ConnBroadcast{Type: "conn", NodeID: c.self.ID, DeviceID: deviceID}
	c.broadcasts.QueueBroadcast(&connBroadcastItem{msg: msg})
}

// BroadcastDisconnect queues a disconnect broadcast via gossip.
func (c *Cluster) BroadcastDisconnect(deviceID string) {
	msg := ConnBroadcast{Type: "disconn", NodeID: c.self.ID, DeviceID: deviceID}
	c.broadcasts.QueueBroadcast(&connBroadcastItem{msg: msg})
}

// SetRemoteConnectHandler sets the callback invoked when a remote node
// claims a device. The broker uses this to disconnect the local session.
func (c *Cluster) SetRemoteConnectHandler(fn func(deviceID, nodeID string)) {
	c.onRemoteConnect.Store(fn)
}

// SetLocalDevicesProvider sets a function that returns locally connected device IDs.
func (c *Cluster) SetLocalDevicesProvider(fn func() []string) {
	c.localDevicesProvider = fn
}

// SetReplicator sets the replicator for offline/will replication.
func (c *Cluster) SetReplicator(r *Replicator) {
	c.replicator = r
}

// Replicator returns the replicator, or nil.
func (c *Cluster) Replicator() *Replicator {
	return c.replicator
}

// SetTakeoverManager sets the takeover manager for session takeover.
func (c *Cluster) SetTakeoverManager(tm *TakeoverManager) {
	c.takeoverManager = tm
}

// TakeoverManager returns the takeover manager, or nil.
func (c *Cluster) TakeoverManager() *TakeoverManager {
	return c.takeoverManager
}

// Transport returns the PeerTransport.
func (c *Cluster) Transport() *PeerTransport {
	return c.transport
}

// Ring returns the hash ring.
func (c *Cluster) Ring() *Ring {
	return c.ring
}

// SelfID returns this node's ID.
func (c *Cluster) SelfID() string {
	return c.self.ID
}

// ReplicaCount returns the configured replica count.
func (c *Cluster) ReplicaCount() int {
	return c.config.ReplicaCount
}

// SetWillPublisher sets the callback for publishing will messages on node death.
func (c *Cluster) SetWillPublisher(fn func(clientID string)) {
	c.willPublisher = fn
}

func (c *Cluster) publishWillsForDeadNode(deviceIDs []string) {
	if c.willPublisher == nil {
		return
	}
	for _, deviceID := range deviceIDs {
		c.willPublisher(deviceID)
	}
}
