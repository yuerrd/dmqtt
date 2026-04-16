package cluster

import (
	"fmt"
	"log"
	"time"
)

// ClusterConfig holds cluster configuration.
type ClusterConfig struct {
	Enabled      bool
	Name         string
	NodeID       string
	Host         string
	GossipPort   int
	MQTTPort     int
	Region       string
	Seeds        []string
	VirtualNodes int
	ReplicaCount int
}

// DefaultClusterConfig returns sensible defaults.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		Enabled:      false,
		Name:         "dmqtt",
		GossipPort:   7000,
		MQTTPort:     1883,
		VirtualNodes: 150,
		ReplicaCount: 3,
	}
}

// Cluster coordinates membership and consistent hashing.
type Cluster struct {
	membership *Membership
	ring       *Ring
	self       NodeInfo
	config     ClusterConfig
	done       chan struct{}
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
		ID:         cfg.NodeID,
		Host:       cfg.Host,
		GossipPort: cfg.GossipPort,
		MQTTPort:   cfg.MQTTPort,
		Region:     cfg.Region,
	}

	ring := NewRing(cfg.VirtualNodes)

	membership, err := NewMembership(self, cfg.Seeds)
	if err != nil {
		return nil, fmt.Errorf("creating membership: %w", err)
	}

	// Initialize ring with current members
	ring.Update(membership.Members())

	c := &Cluster{
		membership: membership,
		ring:       ring,
		self:       self,
		config:     cfg,
		done:       make(chan struct{}),
	}

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
			case NodeLeave:
				log.Printf("cluster: node %s left", ev.Node.ID)
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
	return c.membership.Leave(5 * time.Second)
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
