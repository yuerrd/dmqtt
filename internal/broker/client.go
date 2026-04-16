package broker

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/codec"
	"github.com/langzp/dmqtt/internal/metrics"
)

// Client represents a connected MQTT client.
type Client struct {
	conn      net.Conn
	broker    *Broker
	clientID  string
	will      *WillMessage
	packetIDs *PacketIDAllocator
	inflight  *InflightStore

	mu     sync.Mutex
	closed bool
}

func newClient(conn net.Conn, b *Broker) *Client {
	return &Client{
		conn:      conn,
		broker:    b,
		packetIDs: NewPacketIDAllocator(),
		inflight:  NewInflightStore(b.inflightLimit),
	}
}

// serve handles the client lifecycle: CONNECT then read loop.
func (c *Client) serve() {
	defer c.close()

	if err := c.handleConnect(); err != nil {
		slog.Error("connect error", "remote", c.conn.RemoteAddr().String(), "error", err)
		return
	}

	for {
		fh, data, err := codec.ReadPacket(c.conn)
		if err != nil {
			break
		}

		switch fh.PacketType {
		case codec.PUBLISH:
			c.handlePublish(fh, data)
		case codec.PUBACK:
			c.handlePuback(data)
		case codec.PUBREC:
			c.handlePubrec(data)
		case codec.PUBREL:
			c.handlePubrel(data)
		case codec.PUBCOMP:
			c.handlePubcomp(data)
		case codec.SUBSCRIBE:
			c.handleSubscribe(data)
		case codec.UNSUBSCRIBE:
			c.handleUnsubscribe(data)
		case codec.PINGREQ:
			c.send(codec.EncodePingresp())
		case codec.DISCONNECT:
			c.will = nil // Clean disconnect — do not publish will
			return
		default:
			slog.Warn("unsupported packet type", "client", c.clientID, "type", codec.PacketTypeName(fh.PacketType))
		}
	}
}

func (c *Client) handleConnect() error {
	fh, data, err := codec.ReadPacket(c.conn)
	if err != nil {
		return fmt.Errorf("reading first packet: %w", err)
	}
	if fh.PacketType != codec.CONNECT {
		return fmt.Errorf("expected CONNECT, got %s", codec.PacketTypeName(fh.PacketType))
	}

	pkt, err := codec.DecodeConnectPacket(data)
	if err != nil {
		return fmt.Errorf("decoding CONNECT: %w", err)
	}

	if pkt.ProtocolName != "MQTT" || pkt.ProtocolLevel != 4 {
		connack := &codec.ConnackPacket{ReturnCode: codec.ConnackUnacceptableProtocol}
		c.send(connack.Encode())
		return fmt.Errorf("unsupported protocol: %s level %d", pkt.ProtocolName, pkt.ProtocolLevel)
	}

	if pkt.ClientID == "" && !pkt.CleanSession {
		connack := &codec.ConnackPacket{ReturnCode: codec.ConnackIdentifierRejected}
		c.send(connack.Encode())
		return fmt.Errorf("empty client ID without clean session")
	}

	if pkt.ClientID == "" {
		pkt.ClientID = fmt.Sprintf("auto-%p", c.conn)
	}

	c.clientID = pkt.ClientID

	c.broker.disconnectExisting(c.clientID)

	sessionPresent := false
	existing := c.broker.sessions.Get(c.clientID)
	if pkt.CleanSession || existing == nil {
		c.broker.sessions.Create(c.clientID, pkt.CleanSession)
	} else {
		sessionPresent = true
		for filter, qos := range existing.Subscriptions {
			c.broker.subscriptions.Add(c.clientID, filter, qos)
		}
	}

	if pkt.WillFlag {
		c.will = &WillMessage{
			Topic:   pkt.WillTopic,
			Payload: pkt.WillPayload,
			QoS:     pkt.WillQoS,
			Retain:  pkt.WillRetain,
		}
	}

	c.broker.registerClient(c)

	if c.broker.cluster != nil {
		c.broker.cluster.BroadcastConnect(c.clientID)
	}

	metrics.ConnectionOpened()

	connack := &codec.ConnackPacket{
		SessionPresent: sessionPresent,
		ReturnCode:     codec.ConnackAccepted,
	}
	c.send(connack.Encode())

	if sessionPresent {
		offlineMsgs := c.broker.offlineStore.Dequeue(c.clientID, 100)
		for _, msg := range offlineMsgs {
			c.deliverMessage(msg.Topic, msg.Payload, msg.QoS)
		}
	}

	return nil
}

func (c *Client) handlePublish(fh *codec.FixedHeader, data []byte) {
	pkt, err := codec.DecodePublishPacket(data, fh.QoS)
	if err != nil {
		slog.Error("decode PUBLISH error", "client", c.clientID, "error", err)
		return
	}

	switch fh.QoS {
	case 0:
		// fire and forget
	case 1:
		puback := &codec.PubackPacket{PacketID: pkt.PacketID}
		c.send(puback.Encode())
	case 2:
		if c.broker.dedupStore.IsDuplicate(c.clientID, pkt.PacketID) {
			pubrec := &codec.PubrecPacket{PacketID: pkt.PacketID}
			c.send(pubrec.Encode())
			return
		}
		c.broker.dedupStore.MarkReceived(c.clientID, pkt.PacketID)
		pubrec := &codec.PubrecPacket{PacketID: pkt.PacketID}
		c.send(pubrec.Encode())
	}

	metrics.MessagePublished(fh.QoS)

	if fh.Retain {
		c.broker.retainStore.Set(pkt.Topic, pkt.Payload, fh.QoS)
	}

	c.broker.routeMessage(pkt.Topic, pkt.Payload, fh.QoS, fh.Retain, false)
}

func (c *Client) handlePuback(data []byte) {
	pkt, err := codec.DecodePubackPacket(data)
	if err != nil {
		slog.Error("decode PUBACK error", "client", c.clientID, "error", err)
		return
	}
	c.inflight.Remove(pkt.PacketID)
}

func (c *Client) handlePubrec(data []byte) {
	pkt, err := codec.DecodePubrecPacket(data)
	if err != nil {
		slog.Error("decode PUBREC error", "client", c.clientID, "error", err)
		return
	}
	c.inflight.Remove(pkt.PacketID)
	pubrel := &codec.PubrelPacket{PacketID: pkt.PacketID}
	c.send(pubrel.Encode())
}

func (c *Client) handlePubrel(data []byte) {
	pkt, err := codec.DecodePubrelPacket(data)
	if err != nil {
		slog.Error("decode PUBREL error", "client", c.clientID, "error", err)
		return
	}
	c.broker.dedupStore.Remove(c.clientID, pkt.PacketID)
	pubcomp := &codec.PubcompPacket{PacketID: pkt.PacketID}
	c.send(pubcomp.Encode())
}

func (c *Client) handlePubcomp(data []byte) {
	_, _ = codec.DecodePubcompPacket(data)
}

func (c *Client) deliverMessage(topic string, payload []byte, qos byte) {
	metrics.MessageDelivered(qos)
	
	pkt := &codec.PublishPacket{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  false,
	}

	if qos > 0 {
		pkt.PacketID = c.packetIDs.Next()
		msg := &InflightMessage{
			PacketID:  pkt.PacketID,
			Topic:     topic,
			Payload:   payload,
			QoS:       qos,
			Timestamp: time.Now(),
		}
		if !c.inflight.Add(msg) {
			return
		}
	}

	go c.send(pkt.Encode())
}

func (c *Client) handleSubscribe(data []byte) {
	pkt, err := codec.DecodeSubscribePacket(data)
	if err != nil {
		slog.Error("decode SUBSCRIBE error", "client", c.clientID, "error", err)
		return
	}

	returnCodes := make([]byte, len(pkt.Subscriptions))
	session := c.broker.sessions.Get(c.clientID)

	for i, sub := range pkt.Subscriptions {
		if !TopicFilterValid(sub.TopicFilter) {
			returnCodes[i] = 0x80
			continue
		}

		grantedQoS := sub.QoS
		if grantedQoS > 2 {
			grantedQoS = 2
		}

		c.broker.subscriptions.Add(c.clientID, sub.TopicFilter, grantedQoS)
		if session != nil {
			session.Subscriptions[sub.TopicFilter] = grantedQoS
		}
		returnCodes[i] = grantedQoS
		if c.broker.cluster != nil {
			c.broker.cluster.BroadcastSubscribe(sub.TopicFilter, grantedQoS)
		}
	}

	c.broker.sessions.Save(c.clientID)

	suback := &codec.SubackPacket{
		PacketID:    pkt.PacketID,
		ReturnCodes: returnCodes,
	}
	c.send(suback.Encode())

	// Send retained messages after SUBACK
	for _, sub := range pkt.Subscriptions {
		retained := c.broker.retainStore.Match(sub.TopicFilter)
		for _, msg := range retained {
			pubPkt := &codec.PublishPacket{
				Topic:   msg.Topic,
				QoS:     0,
				Retain:  true,
				Payload: msg.Payload,
			}
			c.send(pubPkt.Encode())
		}
	}
}

func (c *Client) handleUnsubscribe(data []byte) {
	pkt, err := codec.DecodeUnsubscribePacket(data)
	if err != nil {
		slog.Error("decode UNSUBSCRIBE error", "client", c.clientID, "error", err)
		return
	}

	session := c.broker.sessions.Get(c.clientID)
	for _, filter := range pkt.TopicFilters {
		c.broker.subscriptions.Remove(c.clientID, filter)
		if c.broker.cluster != nil {
			c.broker.cluster.BroadcastUnsubscribe(filter)
		}
		if session != nil {
			delete(session.Subscriptions, filter)
		}
	}

	c.broker.sessions.Save(c.clientID)

	unsuback := &codec.UnsubackPacket{PacketID: pkt.PacketID}
	c.send(unsuback.Encode())
}

func (c *Client) send(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.conn.Write(data)
}

func (c *Client) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	c.conn.Close()

	if c.clientID != "" {
		c.broker.unregisterClient(c)

		if c.broker.cluster != nil {
			c.broker.cluster.BroadcastDisconnect(c.clientID)
		}

		metrics.ConnectionClosed()

		if c.will != nil {
			if c.will.Retain {
				c.broker.retainStore.Set(c.will.Topic, c.will.Payload, c.will.QoS)
			}
			c.broker.routeMessage(c.will.Topic, c.will.Payload, c.will.QoS, false, false)
		}

		session := c.broker.sessions.Get(c.clientID)
		if session != nil && session.CleanSession {
			// Broadcast unsubscribe for each filter before removing
			if c.broker.cluster != nil {
				filters := c.broker.subscriptions.ClientFilters(c.clientID)
				for _, filter := range filters {
					c.broker.cluster.BroadcastUnsubscribe(filter)
				}
			}
			c.broker.subscriptions.RemoveAll(c.clientID)
			c.broker.sessions.Remove(c.clientID)
			c.broker.offlineStore.RemoveAll(c.clientID)
		}
	}
}
