package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuerrd/dmqtt/internal/circuitbreaker"
	"github.com/quic-go/quic-go"
)

// MessageType identifies the type of inter-node message.
type MessageType string

const (
	MsgForward     MessageType = "forward"
	MsgForwardAck  MessageType = "forward_ack"
	MsgMigrateData MessageType = "migrate_data"
	MsgMigrateAck  MessageType = "migrate_ack"

	MsgReplicateOffline    MessageType = "replicate_offline"
	MsgReplicateOfflineAck MessageType = "replicate_offline_ack"
	MsgFetchOffline        MessageType = "fetch_offline"
	MsgFetchOfflineResp    MessageType = "fetch_offline_resp"
	MsgReplicateWill       MessageType = "replicate_will"
	MsgReplicateWillAck    MessageType = "replicate_will_ack"
	MsgDeleteWill          MessageType = "delete_will"
	MsgSessionTakeover     MessageType = "session_takeover"
	MsgSessionTakeoverAck  MessageType = "session_takeover_ack"
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

// MigrateOfflineMsg is a single offline message in migration data.
type MigrateOfflineMsg struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     byte   `json:"qos"`
}

// MigrateSessionData is session state transferred during migration.
type MigrateSessionData struct {
	ClientID       string          `json:"client_id"`
	CleanStart     bool            `json:"clean_start"`
	ExpiryInterval uint32          `json:"expiry_interval"`
	Subscriptions  map[string]byte `json:"subscriptions"`
}

// MigrateDataMessage transfers device data from source to target node.
type MigrateDataMessage struct {
	Type     MessageType         `json:"type"`
	DeviceID string              `json:"device_id"`
	Messages []MigrateOfflineMsg `json:"messages"`
	Session  *MigrateSessionData `json:"session,omitempty"`
}

// MigrateAckMessage acknowledges receipt of migration data.
type MigrateAckMessage struct {
	Type     MessageType `json:"type"`
	DeviceID string      `json:"device_id"`
	Success  bool        `json:"success"`
}

// ReplicateOfflineEntry is a single offline message in replication data.
type ReplicateOfflineEntry struct {
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
	QoS       byte   `json:"qos"`
	Priority  int    `json:"priority"`
	CreatedAt int64  `json:"created_at"`
	ExpiresAt int64  `json:"expires_at"`
}

// ReplicateOfflineMessage replicates offline messages to a peer node.
type ReplicateOfflineMessage struct {
	Type     MessageType             `json:"type"`
	ClientID string                  `json:"client_id"`
	Messages []ReplicateOfflineEntry `json:"messages"`
}

// ReplicateOfflineAckMessage acknowledges receipt of replicated offline messages.
type ReplicateOfflineAckMessage struct {
	Type     MessageType `json:"type"`
	ClientID string      `json:"client_id"`
	Success  bool        `json:"success"`
}

// FetchOfflineMessage requests offline messages for a client.
type FetchOfflineMessage struct {
	Type     MessageType `json:"type"`
	ClientID string      `json:"client_id"`
}

// FetchOfflineRespMessage returns offline messages for a client.
type FetchOfflineRespMessage struct {
	Type     MessageType             `json:"type"`
	ClientID string                  `json:"client_id"`
	Messages []ReplicateOfflineEntry `json:"messages"`
}

// ReplicateWillMessage replicates a will message to a peer node.
type ReplicateWillMessage struct {
	Type        MessageType `json:"type"`
	ClientID    string      `json:"client_id"`
	Topic       string      `json:"topic"`
	Payload     []byte      `json:"payload"`
	QoS         byte        `json:"qos"`
	Retain      bool        `json:"retain"`
	PersistedAt int64       `json:"persisted_at"`
}

// ReplicateWillAckMessage acknowledges receipt of a replicated will message.
type ReplicateWillAckMessage struct {
	Type     MessageType `json:"type"`
	ClientID string      `json:"client_id"`
	Success  bool        `json:"success"`
}

// DeleteWillMessage requests deletion of a will message on a peer node.
type DeleteWillMessage struct {
	Type     MessageType `json:"type"`
	ClientID string      `json:"client_id"`
}

// TakeoverRequest requests session takeover of a client.
type TakeoverRequest struct {
	Type             MessageType `json:"type"`
	ClientID         string      `json:"client_id"`
	RequestNodeID    string      `json:"request_node_id"`
	ConnectTimestamp int64       `json:"connect_timestamp"`
	Epoch            uint64      `json:"epoch"`
}

// TakeoverResponse responds to a session takeover request.
type TakeoverResponse struct {
	Type     MessageType `json:"type"`
	ClientID string      `json:"client_id"`
	Success  bool        `json:"success"`
	Reason   string      `json:"reason,omitempty"`
}

// HandlerRegistry provides a thread-safe registry for dynamic message handlers.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[MessageType]func([]byte)
}

// NewHandlerRegistry creates a new HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[MessageType]func([]byte)),
	}
}

// Register adds a handler for a given message type.
func (r *HandlerRegistry) Register(msgType MessageType, fn func([]byte)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgType] = fn
}

// Dispatch invokes the registered handler for the given message type.
// Returns true if a handler was found, false otherwise.
func (r *HandlerRegistry) Dispatch(msgType MessageType, data []byte) bool {
	r.mu.RLock()
	fn, ok := r.handlers[msgType]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	fn(data)
	return true
}

// PeerTransport manages persistent QUIC connections between cluster nodes.
type PeerTransport struct {
	listenAddr   string
	selfID       string
	quicListener *quic.Listener
	serverTLS    *tls.Config
	clientTLS    *tls.Config
	handler      func(ForwardMessage)
	msgID        atomic.Uint64

	mu    sync.RWMutex
	peers map[string]*peerConn
	done  chan struct{}

	// ackWaiters tracks pending ACK channels by message ID
	ackMu      sync.Mutex
	ackWaiters map[uint64]chan struct{}

	// cbConfig stores the circuit breaker configuration for new peers
	cbConfig *circuitbreaker.Config

	migrateHandler func(MigrateDataMessage)

	registry *HandlerRegistry
}

type peerConn struct {
	nodeID  string
	addr    string
	qconn   *quic.Conn
	stream  *quic.Stream
	mu      sync.Mutex
	closed  bool
	done    chan struct{}
	breaker *circuitbreaker.CircuitBreaker
}

// generateSelfSignedTLSConfig creates a self-signed TLS config using ECDSA P-256.
func generateSelfSignedTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  key,
		}},
		NextProtos: []string{"dmqtt-peer"},
	}, nil
}

// NewPeerTransport creates and starts a PeerTransport listening on the given address.
func NewPeerTransport(listenAddr, selfID string, handler func(ForwardMessage)) (*PeerTransport, error) {
	serverTLS, err := generateSelfSignedTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("peer transport generate TLS config: %w", err)
	}

	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"dmqtt-peer"},
	}

	ln, err := quic.ListenAddr(listenAddr, serverTLS, nil)
	if err != nil {
		return nil, fmt.Errorf("peer transport listen: %w", err)
	}

	pt := &PeerTransport{
		listenAddr:   ln.Addr().String(),
		selfID:       selfID,
		quicListener: ln,
		serverTLS:    serverTLS,
		clientTLS:    clientTLS,
		handler:      handler,
		peers:        make(map[string]*peerConn),
		done:         make(chan struct{}),
		ackWaiters:   make(map[uint64]chan struct{}),
		registry:     NewHandlerRegistry(),
	}

	go pt.acceptLoop()
	go pt.cleanupStaleAckWaiters()
	return pt, nil
}

// SetMigrateHandler sets the callback for incoming migration data.
func (pt *PeerTransport) SetMigrateHandler(fn func(MigrateDataMessage)) {
	pt.migrateHandler = fn
}

// RegisterHandler registers a dynamic handler for a given message type.
func (pt *PeerTransport) RegisterHandler(msgType MessageType, fn func([]byte)) {
	pt.registry.Register(msgType, fn)
}

// SendRaw marshals msg to JSON and sends it to the given peer.
func (pt *PeerTransport) SendRaw(nodeID string, msg interface{}) error {
	pt.mu.RLock()
	pc, ok := pt.peers[nodeID]
	pt.mu.RUnlock()

	if !ok {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	// Circuit breaker check
	if pc.breaker != nil {
		if err := pc.breaker.AllowOrError(); err != nil {
			return err
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal raw message: %w", err)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.stream == nil {
		if pc.breaker != nil {
			pc.breaker.RecordFailure()
		}
		return fmt.Errorf("peer %s not connected", nodeID)
	}

	err = writeFrame(pc.stream, data)
	if pc.breaker != nil {
		if err != nil {
			pc.breaker.RecordFailure()
		} else {
			pc.breaker.RecordSuccess()
		}
	}
	return err
}

func (pt *PeerTransport) acceptLoop() {
	for {
		qconn, err := pt.quicListener.Accept(context.Background())
		if err != nil {
			select {
			case <-pt.done:
				return
			default:
				slog.Error("peer transport accept error", "error", err)
				continue
			}
		}
		go pt.handleQUICConn(qconn)
	}
}

// cleanupStaleAckWaiters periodically removes ack waiters that have been
// pending for longer than the maximum expected timeout.
func (pt *PeerTransport) cleanupStaleAckWaiters() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pt.ackMu.Lock()
			staleCount := len(pt.ackWaiters)
			if staleCount > 0 {
				slog.Debug("cleaning up stale ack waiters", "count", staleCount)
			}
			for id, ch := range pt.ackWaiters {
				select {
				case <-ch:
					// Already closed, just delete
				default:
					close(ch)
				}
				delete(pt.ackWaiters, id)
			}
			pt.ackMu.Unlock()
		case <-pt.done:
			return
		}
	}
}

func (pt *PeerTransport) handleQUICConn(qconn *quic.Conn) {
	defer qconn.CloseWithError(0, "done")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := qconn.AcceptStream(ctx)
	if err != nil {
		select {
		case <-pt.done:
		default:
			slog.Error("peer transport accept stream error", "error", err)
		}
		return
	}
	pt.handleConn(stream)
}

func (pt *PeerTransport) handleConn(conn io.ReadWriteCloser) {
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
					slog.Error("peer transport read error", "error", err)
				}
			}
			return
		}

		// Peek at message type
		var peek struct {
			Type MessageType `json:"type"`
		}
		if err := json.Unmarshal(data, &peek); err != nil {
			slog.Error("peer transport unmarshal peek error", "error", err)
			continue
		}

		switch peek.Type {
		case MsgForward:
			var msg ForwardMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				slog.Error("peer transport unmarshal forward error", "error", err)
				continue
			}
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
		case MsgMigrateData:
			var migrate MigrateDataMessage
			if err := json.Unmarshal(data, &migrate); err != nil {
				slog.Error("peer transport unmarshal migrate error", "error", err)
				continue
			}
			ack := MigrateAckMessage{Type: MsgMigrateAck, DeviceID: migrate.DeviceID, Success: true}
			ackData, _ := json.Marshal(ack)
			writeFrame(conn, ackData)
			if pt.migrateHandler != nil {
				pt.migrateHandler(migrate)
			}
		case MsgMigrateAck:
			var ack MigrateAckMessage
			json.Unmarshal(data, &ack)
			pt.ackMu.Lock()
			key := uint64(0)
			for _, b := range []byte(ack.DeviceID) {
				key = key*31 + uint64(b)
			}
			if ch, ok := pt.ackWaiters[key]; ok {
				close(ch)
				delete(pt.ackWaiters, key)
			}
			pt.ackMu.Unlock()
		default:
			if !pt.registry.Dispatch(peek.Type, data) {
				slog.Warn("peer transport: unknown message type", "type", peek.Type)
			}
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
	if pt.cbConfig != nil {
		pc.breaker = circuitbreaker.New(*pt.cbConfig)
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		qconn, err := quic.DialAddr(ctx, pc.addr, pt.clientTLS, nil)
		cancel()
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

		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := qconn.OpenStreamSync(ctx2)
		cancel2()
		if err != nil {
			qconn.CloseWithError(0, "stream open failed")
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
		pc.qconn = qconn
		pc.stream = stream
		pc.mu.Unlock()

		// Read ACKs from this outbound connection
		pt.handleConn(stream)

		// Connection lost — reset and try reconnecting
		pc.mu.Lock()
		if pc.qconn != nil {
			pc.qconn.CloseWithError(0, "reconnecting")
		}
		pc.qconn = nil
		pc.stream = nil
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
		if pc.qconn != nil {
			pc.qconn.CloseWithError(0, "peer removed")
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

	// Circuit breaker check
	if pc.breaker != nil {
		if err := pc.breaker.AllowOrError(); err != nil {
			return err
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal forward message: %w", err)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.stream == nil {
		if pc.breaker != nil {
			pc.breaker.RecordFailure()
		}
		return fmt.Errorf("peer %s not connected", nodeID)
	}

	err = writeFrame(pc.stream, data)
	if pc.breaker != nil {
		if err != nil {
			pc.breaker.RecordFailure()
		} else {
			pc.breaker.RecordSuccess()
		}
	}
	return err
}

// SendReliable sends a forward message and waits for an ACK.
// Retries up to maxRetries times with the given timeout per attempt.
func (pt *PeerTransport) SendReliable(nodeID string, msg ForwardMessage, maxRetries int, timeout time.Duration) error {
	pt.mu.RLock()
	pc, ok := pt.peers[nodeID]
	pt.mu.RUnlock()
	if ok && pc.breaker != nil {
		if err := pc.breaker.AllowOrError(); err != nil {
			return err
		}
	}

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
			if err == circuitbreaker.ErrCircuitOpen {
				return err
			}
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
				slog.Warn("peer transport: ACK timeout", "msgID", msg.ID, "peer", nodeID, "retry", attempt+1, "maxRetries", maxRetries)
				continue
			}
			return fmt.Errorf("ACK timeout for msg %d to %s after %d attempts", msg.ID, nodeID, maxRetries+1)
		case <-pt.done:
			return fmt.Errorf("transport shutting down")
		}
	}
	return nil
}

// SendMigrate sends migration data to a peer and waits for ACK.
func (pt *PeerTransport) SendMigrate(nodeID string, msg MigrateDataMessage) error {
	msg.Type = MsgMigrateData
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal migrate message: %w", err)
	}

	pt.mu.RLock()
	pc, ok := pt.peers[nodeID]
	pt.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not found", nodeID)
	}

	// Register ACK waiter using deviceID hash
	key := uint64(0)
	for _, b := range []byte(msg.DeviceID) {
		key = key*31 + uint64(b)
	}
	ackCh := make(chan struct{})
	pt.ackMu.Lock()
	pt.ackWaiters[key] = ackCh
	pt.ackMu.Unlock()

	pc.mu.Lock()
	if pc.stream == nil {
		pc.mu.Unlock()
		pt.ackMu.Lock()
		delete(pt.ackWaiters, key)
		pt.ackMu.Unlock()
		return fmt.Errorf("peer %s not connected", nodeID)
	}
	err = writeFrame(pc.stream, data)
	pc.mu.Unlock()
	if err != nil {
		pt.ackMu.Lock()
		delete(pt.ackWaiters, key)
		pt.ackMu.Unlock()
		return fmt.Errorf("write migrate message: %w", err)
	}

	select {
	case <-ackCh:
		return nil
	case <-time.After(10 * time.Second):
		pt.ackMu.Lock()
		delete(pt.ackWaiters, key)
		pt.ackMu.Unlock()
		return fmt.Errorf("migrate ACK timeout for device %s to %s", msg.DeviceID, nodeID)
	case <-pt.done:
		return fmt.Errorf("transport shutting down")
	}
}

// Stop shuts down the transport, closing all connections.
func (pt *PeerTransport) Stop() error {
	close(pt.done)
	pt.quicListener.Close()

	pt.mu.Lock()
	for nodeID, pc := range pt.peers {
		pc.mu.Lock()
		if !pc.closed {
			close(pc.done)
			pc.closed = true
		}
		if pc.qconn != nil {
			pc.qconn.CloseWithError(0, "transport stopping")
		}
		pc.mu.Unlock()
		delete(pt.peers, nodeID)
	}
	pt.mu.Unlock()

	return nil
}

// SetCircuitBreakerConfig sets the circuit breaker config for all new peers.
// Existing peers are updated with new breakers using the new config.
func (pt *PeerTransport) SetCircuitBreakerConfig(cfg circuitbreaker.Config) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.cbConfig = &cfg
	for _, pc := range pt.peers {
		pc.breaker = circuitbreaker.New(cfg)
	}
}

// --- Wire protocol: [4-byte big-endian length][JSON payload] ---

func writeFrame(w io.Writer, data []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > 10*1024*1024 { // 10MB sanity limit
		return nil, fmt.Errorf("frame too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
