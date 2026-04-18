package cluster

import (
	"log/slog"
	"sync"
	"time"
)

// RawSender is the interface Replicator uses to send messages to peers.
type RawSender interface {
	SendRaw(nodeID string, msg interface{}) error
}

// ReplicaLocator finds replica nodes for a device.
type ReplicaLocator interface {
	LocateDeviceN(deviceID string, n int) []NodeInfo
}

// Replicator handles async replication of offline messages and will messages.
type Replicator struct {
	selfID       string
	transport    RawSender
	ring         ReplicaLocator
	replicaCount int
	wg           sync.WaitGroup
}

func NewReplicator(selfID string, transport RawSender, ring ReplicaLocator, replicaCount int) *Replicator {
	return &Replicator{
		selfID:       selfID,
		transport:    transport,
		ring:         ring,
		replicaCount: replicaCount,
	}
}

func (r *Replicator) replicaNodes(clientID string) []NodeInfo {
	nodes := r.ring.LocateDeviceN(clientID, r.replicaCount)
	var replicas []NodeInfo
	for _, n := range nodes {
		if n.ID != r.selfID {
			replicas = append(replicas, n)
		}
	}
	return replicas
}

func (r *Replicator) ReplicateOffline(clientID string, entries []ReplicateOfflineEntry) {
	replicas := r.replicaNodes(clientID)
	if len(replicas) == 0 {
		return
	}

	msg := ReplicateOfflineMessage{
		Type:     MsgReplicateOffline,
		ClientID: clientID,
		Messages: entries,
	}

	for _, node := range replicas {
		r.wg.Add(1)
		go func(nodeID string) {
			defer r.wg.Done()
			if err := r.transport.SendRaw(nodeID, msg); err != nil {
				slog.Warn("replicate offline failed", "client", clientID, "peer", nodeID, "error", err)
			}
		}(node.ID)
	}
}

func (r *Replicator) ClearOfflineReplica(clientID string) {
	replicas := r.replicaNodes(clientID)
	if len(replicas) == 0 {
		return
	}

	msg := ReplicateOfflineAckMessage{
		Type:     MsgReplicateOfflineAck,
		ClientID: clientID,
		Success:  true,
	}

	for _, node := range replicas {
		r.wg.Add(1)
		go func(nodeID string) {
			defer r.wg.Done()
			if err := r.transport.SendRaw(nodeID, msg); err != nil {
				slog.Warn("clear offline replica failed", "client", clientID, "peer", nodeID, "error", err)
			}
		}(node.ID)
	}
}

func (r *Replicator) ReplicateWill(clientID, topic string, payload []byte, qos byte, retain bool) {
	replicas := r.replicaNodes(clientID)
	if len(replicas) == 0 {
		return
	}

	msg := ReplicateWillMessage{
		Type:        MsgReplicateWill,
		ClientID:    clientID,
		Topic:       topic,
		Payload:     payload,
		QoS:         qos,
		Retain:      retain,
		PersistedAt: time.Now().UnixNano(),
	}

	for _, node := range replicas {
		r.wg.Add(1)
		go func(nodeID string) {
			defer r.wg.Done()
			if err := r.transport.SendRaw(nodeID, msg); err != nil {
				slog.Warn("replicate will failed", "client", clientID, "peer", nodeID, "error", err)
			}
		}(node.ID)
	}
}

// Stop waits for all in-flight replication goroutines to finish.
func (r *Replicator) Stop() {
	r.wg.Wait()
}

func (r *Replicator) DeleteWill(clientID string) {
	replicas := r.replicaNodes(clientID)
	if len(replicas) == 0 {
		return
	}

	msg := DeleteWillMessage{
		Type:     MsgDeleteWill,
		ClientID: clientID,
	}

	for _, node := range replicas {
		r.wg.Add(1)
		go func(nodeID string) {
			defer r.wg.Done()
			if err := r.transport.SendRaw(nodeID, msg); err != nil {
				slog.Warn("delete will replica failed", "client", clientID, "peer", nodeID, "error", err)
			}
		}(node.ID)
	}
}
