package cluster

import (
	"encoding/json"
	"fmt"
	"log"
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
	onRemoteConnect      func(deviceID, nodeID string)

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
			switch ev.Type {
			case NodeJoin:
				log.Printf("cluster: node %s joined", ev.Node.ID)
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
				log.Printf("cluster: node %s left", ev.Node.ID)
				c.transport.RemovePeer(ev.Node.ID)
				c.remoteSubs.RemoveNode(ev.Node.ID)
				c.connections.RemoveNode(ev.Node.ID)
			case NodeUpdate:
				log.Printf("cluster: node %s updated", ev.Node.ID)
			}
			c.ring.Update(c.membership.Members())
		case <-c.done:
			return
		}
	}
}

// Stop gracefully leaves the cluster and shuts down.
func (c *Cluster) Stop() error {
	close(c.done)
	if c.transport != nil {
		c.transport.Stop()
	}
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
		log.Printf("cluster: unmarshal broadcast peek: %v", err)
		return
	}

	switch peek.Type {
	case "sub", "unsub":
		var msg SubBroadcast
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("cluster: unmarshal sub broadcast: %v", err)
			return
		}
		if msg.NodeID == c.self.ID {
			return
		}
		HandleSubBroadcast(c.remoteSubs, msg)
	case "conn", "disconn":
		var msg ConnBroadcast
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("cluster: unmarshal conn broadcast: %v", err)
			return
		}
		if msg.NodeID == c.self.ID {
			return
		}
		HandleConnBroadcast(c.connections, msg)
		if msg.Type == "conn" && c.onRemoteConnect != nil {
			c.onRemoteConnect(msg.DeviceID, msg.NodeID)
		}
	default:
		log.Printf("cluster: unknown broadcast type: %s", peek.Type)
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
	c.onRemoteConnect = fn
}

// SetLocalDevicesProvider sets a function that returns locally connected device IDs.
func (c *Cluster) SetLocalDevicesProvider(fn func() []string) {
	c.localDevicesProvider = fn
}
