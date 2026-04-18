package cluster

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Disconnector disconnects a local client.
type Disconnector interface {
	DisconnectDevice(clientID string)
}

// TakeoverManager handles atomic session takeover between cluster nodes.
type TakeoverManager struct {
	selfID       string
	transport    RawSender
	disconnector Disconnector
	resolver     ConflictResolver
	timeout      time.Duration

	mu       sync.RWMutex
	sessions map[string]*SessionMeta

	ackMu    sync.Mutex
	ackChans map[string]chan TakeoverResponse
}

func NewTakeoverManager(selfID string, transport RawSender, disconnector Disconnector, resolver ConflictResolver, timeout time.Duration) *TakeoverManager {
	return &TakeoverManager{
		selfID:       selfID,
		transport:    transport,
		disconnector: disconnector,
		resolver:     resolver,
		timeout:      timeout,
		sessions:     make(map[string]*SessionMeta),
		ackChans:     make(map[string]chan TakeoverResponse),
	}
}

func (tm *TakeoverManager) SetLocalSession(clientID string, meta *SessionMeta) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.sessions[clientID] = meta
}

func (tm *TakeoverManager) RemoveLocalSession(clientID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.sessions, clientID)
}

func (tm *TakeoverManager) RequestTakeover(clientID, targetNodeID string, epoch uint64, connectTimestamp int64) bool {
	req := TakeoverRequest{
		Type:             MsgSessionTakeover,
		ClientID:         clientID,
		RequestNodeID:    tm.selfID,
		ConnectTimestamp: connectTimestamp,
		Epoch:            epoch,
	}

	respCh := make(chan TakeoverResponse, 1)
	tm.ackMu.Lock()
	if oldCh, exists := tm.ackChans[clientID]; exists {
		close(oldCh)
	}
	tm.ackChans[clientID] = respCh
	tm.ackMu.Unlock()

	defer func() {
		tm.ackMu.Lock()
		delete(tm.ackChans, clientID)
		tm.ackMu.Unlock()
	}()

	if err := tm.transport.SendRaw(targetNodeID, req); err != nil {
		slog.Warn("takeover request failed", "client", clientID, "target", targetNodeID, "error", err)
		return true // Assume dead node
	}

	select {
	case resp, ok := <-respCh:
		if !ok {
			// Channel closed by a newer RequestTakeover — treat as takeover success
			return true
		}
		return resp.Success
	case <-time.After(tm.timeout):
		slog.Warn("takeover timeout, assuming dead node", "client", clientID, "target", targetNodeID)
		return true
	}
}

func (tm *TakeoverManager) HandleTakeoverRequest(req TakeoverRequest) {
	remoteMeta := &SessionMeta{
		ClientID:        req.ClientID,
		Epoch:           req.Epoch,
		ConnectTimestamp: req.ConnectTimestamp,
		NodeID:          req.RequestNodeID,
	}

	tm.mu.Lock()
	localMeta, exists := tm.sessions[req.ClientID]

	shouldYield := !exists || tm.resolver.Resolve(localMeta, remoteMeta)

	if shouldYield && exists {
		delete(tm.sessions, req.ClientID)
	}
	tm.mu.Unlock()

	if shouldYield {
		if exists {
			tm.disconnector.DisconnectDevice(req.ClientID)
		}
		resp := TakeoverResponse{
			Type:     MsgSessionTakeoverAck,
			ClientID: req.ClientID,
			Success:  true,
		}
		if err := tm.transport.SendRaw(req.RequestNodeID, resp); err != nil {
			slog.Warn("takeover ACK failed", "client", req.ClientID, "target", req.RequestNodeID, "error", err)
		}
	} else {
		resp := TakeoverResponse{
			Type:     MsgSessionTakeoverAck,
			ClientID: req.ClientID,
			Success:  false,
			Reason:   "local session wins conflict resolution",
		}
		if err := tm.transport.SendRaw(req.RequestNodeID, resp); err != nil {
			slog.Warn("takeover NACK failed", "client", req.ClientID, "target", req.RequestNodeID, "error", err)
		}
	}
}

func (tm *TakeoverManager) HandleTakeoverResponse(data []byte) {
	var resp TakeoverResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		slog.Error("unmarshal takeover response", "error", err)
		return
	}

	tm.ackMu.Lock()
	ch, ok := tm.ackChans[resp.ClientID]
	tm.ackMu.Unlock()

	if ok {
		select {
		case ch <- resp:
		default:
		}
	}
}
