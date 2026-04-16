package cluster

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// EventType represents a cluster membership event.
type EventType int

const (
	NodeJoin EventType = iota
	NodeLeave
	NodeUpdate
)

// MemberEvent represents a membership change.
type MemberEvent struct {
	Type EventType
	Node NodeInfo
}

// Membership manages cluster membership via SWIM gossip.
type Membership struct {
	mu       sync.RWMutex
	list     *memberlist.Memberlist
	self     NodeInfo
	events   chan MemberEvent
	delegate *membershipDelegate
}

// NewMembership creates a new Membership for the given node.
// seeds is a list of existing node gossip addresses to join (can be empty for first node).
func NewMembership(self NodeInfo, seeds []string) (*Membership, error) {
	m := &Membership{
		self:   self,
		events: make(chan MemberEvent, 256),
	}

	m.delegate = &membershipDelegate{
		self:   self,
		events: m.events,
	}

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = self.ID
	cfg.BindAddr = self.Host
	cfg.BindPort = self.GossipPort
	cfg.AdvertisePort = self.GossipPort
	cfg.Delegate = m.delegate
	cfg.Events = m.delegate
	cfg.LogOutput = log.Writer()

	// Reduce chattiness for small clusters
	cfg.GossipInterval = 500 * time.Millisecond
	cfg.ProbeInterval = 1 * time.Second

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating memberlist: %w", err)
	}
	m.list = list

	if len(seeds) > 0 {
		n, err := list.Join(seeds)
		if err != nil {
			list.Shutdown()
			return nil, fmt.Errorf("joining cluster via %v: %w", seeds, err)
		}
		log.Printf("joined cluster via %d seed(s)", n)
	}

	return m, nil
}

// Members returns info about all live members including self.
func (m *Membership) Members() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := m.list.Members()
	nodes := make([]NodeInfo, 0, len(members))
	for _, member := range members {
		info, err := UnmarshalNodeInfo(member.Meta)
		if err != nil {
			log.Printf("skipping member %s: bad metadata: %v", member.Name, err)
			continue
		}
		nodes = append(nodes, info)
	}
	return nodes
}

// Events returns the channel of membership events.
func (m *Membership) Events() <-chan MemberEvent {
	return m.events
}

// Leave gracefully leaves the cluster.
func (m *Membership) Leave(timeout time.Duration) error {
	if err := m.list.Leave(timeout); err != nil {
		return err
	}
	return m.list.Shutdown()
}

// Self returns this node's info.
func (m *Membership) Self() NodeInfo {
	return m.self
}

// membershipDelegate implements memberlist.Delegate and memberlist.EventDelegate.
type membershipDelegate struct {
	self   NodeInfo
	meta   []byte
	events chan MemberEvent
}

// --- memberlist.Delegate interface ---

func (d *membershipDelegate) NodeMeta(limit int) []byte {
	data, _ := d.self.Marshal()
	if len(data) > limit {
		log.Printf("WARNING: node metadata %d bytes exceeds limit %d", len(data), limit)
		return nil
	}
	return data
}

func (d *membershipDelegate) NotifyMsg([]byte)                            {}
func (d *membershipDelegate) GetBroadcasts(overhead, limit int) [][]byte  { return nil }
func (d *membershipDelegate) LocalState(join bool) []byte                 { return nil }
func (d *membershipDelegate) MergeRemoteState(buf []byte, join bool)      {}

// --- memberlist.EventDelegate interface ---

func (d *membershipDelegate) NotifyJoin(node *memberlist.Node) {
	info, err := UnmarshalNodeInfo(node.Meta)
	if err != nil {
		return
	}
	select {
	case d.events <- MemberEvent{Type: NodeJoin, Node: info}:
	default:
		log.Printf("membership event channel full, dropping join event for %s", node.Name)
	}
}

func (d *membershipDelegate) NotifyLeave(node *memberlist.Node) {
	info, err := UnmarshalNodeInfo(node.Meta)
	if err != nil {
		// Best effort: use just the node name as ID
		info = NodeInfo{ID: node.Name}
	}
	select {
	case d.events <- MemberEvent{Type: NodeLeave, Node: info}:
	default:
		log.Printf("membership event channel full, dropping leave event for %s", node.Name)
	}
}

func (d *membershipDelegate) NotifyUpdate(node *memberlist.Node) {
	info, err := UnmarshalNodeInfo(node.Meta)
	if err != nil {
		return
	}
	select {
	case d.events <- MemberEvent{Type: NodeUpdate, Node: info}:
	default:
	}
}
