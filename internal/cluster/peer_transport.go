package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// MessageType identifies the type of inter-node message.
type MessageType string

const (
	MsgForward    MessageType = "forward"
	MsgForwardAck MessageType = "forward_ack"
)

// ForwardMessage is sent between nodes to forward a published message.
type ForwardMessage struct {
	Type      MessageType `json:"type"`
	ID        uint64      `json:"id"`
	Topic     string      `json:"topic"`
	Payload   []byte      `json:"payload"`
	QoS       byte        `json:"qos"`
	Retain    bool        `json:"retain"`
	Forwarded bool        `json:"forwarded"`
}

// ForwardAckMessage acknowledges receipt of a QoS 1/2 forwarded message.
type ForwardAckMessage struct {
	Type MessageType `json:"type"`
	ID   uint64      `json:"id"`
}

// PeerTransport manages persistent TCP connections between cluster nodes.
type PeerTransport struct {
	listenAddr string
	selfID     string
	listener   net.Listener
	handler    func(ForwardMessage)
	msgID      atomic.Uint64

	mu    sync.RWMutex
	peers map[string]*peerConn
	done  chan struct{}

	// ackWaiters tracks pending ACK channels by message ID
	ackMu      sync.Mutex
	ackWaiters map[uint64]chan struct{}
}

type peerConn struct {
	nodeID string
	addr   string
	conn   net.Conn
	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

// NewPeerTransport creates and starts a PeerTransport listening on the given address.
func NewPeerTransport(listenAddr, selfID string, handler func(ForwardMessage)) (*PeerTransport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("peer transport listen: %w", err)
	}

	pt := &PeerTransport{
		listenAddr: listenAddr,
		selfID:     selfID,
		listener:   ln,
		handler:    handler,
		peers:      make(map[string]*peerConn),
		done:       make(chan struct{}),
		ackWaiters: make(map[uint64]chan struct{}),
	}

	go pt.acceptLoop()
	return pt, nil
}

func (pt *PeerTransport) acceptLoop() {
	for {
		conn, err := pt.listener.Accept()
		if err != nil {
			select {
			case <-pt.done:
				return
			default:
				log.Printf("peer transport accept error: %v", err)
				continue
			}
		}
		go pt.handleConn(conn)
	}
}

func (pt *PeerTransport) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-pt.done:
			return
		default:
		}

		data, err := readFrame(conn)
		if err != nil {
			if err != io.EOF {
				select {
				case <-pt.done:
				default:
					log.Printf("peer transport read error: %v", err)
				}
			}
			return
		}

		// Try to decode as ForwardMessage first
		var msg ForwardMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("peer transport unmarshal error: %v", err)
			continue
		}

		switch msg.Type {
		case MsgForward:
			// Auto-ACK for QoS > 0
			if msg.QoS > 0 {
				ack := ForwardAckMessage{Type: MsgForwardAck, ID: msg.ID}
				ackData, _ := json.Marshal(ack)
				writeFrame(conn, ackData)
			}
			if pt.handler != nil {
				pt.handler(msg)
			}
		case MsgForwardAck:
			var ack ForwardAckMessage
			json.Unmarshal(data, &ack)
			pt.ackMu.Lock()
			if ch, ok := pt.ackWaiters[ack.ID]; ok {
				close(ch)
				delete(pt.ackWaiters, ack.ID)
			}
			pt.ackMu.Unlock()
		}
	}
}

// AddPeer registers a peer node and starts connecting to it.
func (pt *PeerTransport) AddPeer(nodeID, addr string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if _, ok := pt.peers[nodeID]; ok {
		return
	}

	pc := &peerConn{
		nodeID: nodeID,
		addr:   addr,
		done:   make(chan struct{}),
	}
	pt.peers[nodeID] = pc

	go pt.connectPeer(pc)
}

func (pt *PeerTransport) connectPeer(pc *peerConn) {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-pt.done:
			return
		case <-pc.done:
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", pc.addr, 5*time.Second)
		if err != nil {
			select {
			case <-pt.done:
				return
			case <-pc.done:
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}

		pc.mu.Lock()
		pc.conn = conn
		pc.mu.Unlock()

		// Read ACKs from this outbound connection
		pt.handleConn(conn)

		// Connection lost — reset and try reconnecting
		pc.mu.Lock()
		pc.conn = nil
		pc.mu.Unlock()
		backoff = time.Second
	}
}

// RemovePeer disconnects and removes a peer.
func (pt *PeerTransport) RemovePeer(nodeID string) {
	pt.mu.Lock()
	pc, ok := pt.peers[nodeID]
	if ok {
		delete(pt.peers, nodeID)
	}
	pt.mu.Unlock()

	if ok {
		close(pc.done)
		pc.mu.Lock()
		if pc.conn != nil {
			pc.conn.Close()
		}
		pc.closed = true
		pc.mu.Unlock()
	}
}

// NextID returns a monotonically increasing message ID.
func (pt *PeerTransport) NextID() uint64 {
	return pt.msgID.Add(1)
}

// Send sends a fire-and-forget forward message to a peer.
func (pt *PeerTransport) Send(nodeID string, msg ForwardMessage) error {
	pt.mu.RLock()
	pc, ok := pt.peers[nodeID]
	pt.mu.RUnlock()

	if !ok {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal forward message: %w", err)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn == nil {
		return fmt.Errorf("peer %s not connected", nodeID)
	}

	return writeFrame(pc.conn, data)
}

// SendReliable sends a forward message and waits for an ACK.
// Retries up to maxRetries times with the given timeout per attempt.
func (pt *PeerTransport) SendReliable(nodeID string, msg ForwardMessage, maxRetries int, timeout time.Duration) error {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		ackCh := make(chan struct{})
		pt.ackMu.Lock()
		pt.ackWaiters[msg.ID] = ackCh
		pt.ackMu.Unlock()

		err := pt.Send(nodeID, msg)
		if err != nil {
			pt.ackMu.Lock()
			delete(pt.ackWaiters, msg.ID)
			pt.ackMu.Unlock()
			if attempt < maxRetries {
				continue
			}
			return fmt.Errorf("send to %s failed after %d attempts: %w", nodeID, attempt+1, err)
		}

		select {
		case <-ackCh:
			return nil
		case <-time.After(timeout):
			pt.ackMu.Lock()
			delete(pt.ackWaiters, msg.ID)
			pt.ackMu.Unlock()
			if attempt < maxRetries {
				log.Printf("peer transport: ACK timeout for msg %d to %s, retry %d/%d",
					msg.ID, nodeID, attempt+1, maxRetries)
				continue
			}
			return fmt.Errorf("ACK timeout for msg %d to %s after %d attempts", msg.ID, nodeID, maxRetries+1)
		case <-pt.done:
			return fmt.Errorf("transport shutting down")
		}
	}
	return nil
}

// Stop shuts down the transport, closing all connections.
func (pt *PeerTransport) Stop() error {
	close(pt.done)
	pt.listener.Close()

	pt.mu.Lock()
	for nodeID, pc := range pt.peers {
		close(pc.done)
		pc.mu.Lock()
		if pc.conn != nil {
			pc.conn.Close()
		}
		pc.closed = true
		pc.mu.Unlock()
		delete(pt.peers, nodeID)
	}
	pt.mu.Unlock()

	return nil
}

// --- Wire protocol: [4-byte big-endian length][JSON payload] ---

func writeFrame(conn net.Conn, data []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func readFrame(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > 10*1024*1024 { // 10MB sanity limit
		return nil, fmt.Errorf("frame too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	return data, nil
}
