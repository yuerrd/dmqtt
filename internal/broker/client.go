package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/codec"
	"github.com/langzp/dmqtt/internal/metrics"
	"github.com/langzp/dmqtt/internal/plugin"
)

// Client represents a connected MQTT client.
type Client struct {
	conn            net.Conn
	broker          *Broker
	clientID        string
	username        string
	will            *WillMessage
	packetIDs       *PacketIDAllocator
	inflight        *InflightStore
	keepAlive       time.Duration
	protocolVersion byte

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
		if c.keepAlive > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.keepAlive))
		}
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

	if !c.broker.authenticator.Authenticate(pkt.Username, pkt.Password) {
		connack := &codec.ConnackPacket{ReturnCode: codec.ConnackBadUsernameOrPassword}
		c.send(connack.Encode())
		metrics.AuthAttempt("failure")
		return fmt.Errorf("authentication failed for user %q", pkt.Username)
	}
	metrics.AuthAttempt("success")
	c.username = pkt.Username

	c.clientID = pkt.ClientID
	c.protocolVersion = pkt.ProtocolLevel

	if pkt.KeepAlive > 0 {
		c.keepAlive = time.Duration(float64(pkt.KeepAlive)*1.5) * time.Second
	}

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

	if c.broker.interceptors != nil {
		evt := &plugin.ConnectEvent{
			ClientID:     c.clientID,
			Username:     c.username,
			CleanSession: pkt.CleanSession,
			RemoteAddr:   c.conn.RemoteAddr().String(),
		}
		if err := c.broker.interceptors.OnConnect(context.Background(), evt); err != nil {
			slog.Info("connect rejected by interceptor", "client", c.clientID, "error", err)
			connack := &codec.ConnackPacket{ReturnCode: codec.ConnackNotAuthorized}
			c.send(connack.Encode())
			return fmt.Errorf("interceptor rejected: %w", err)
		}
	}

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
	pkt, err := codec.DecodePublishPacket(data, fh.QoS, 4)
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

	if !c.broker.authorizer.Authorize(c.username, pkt.Topic, "publish") {
		metrics.ACLDenial("publish")
		return
	}

	if err := c.broker.rateLimiter.AllowPublish(c.clientID, pkt.Topic, len(pkt.Payload)); err != nil {
		slog.Debug("publish rate limited", "client", c.clientID, "topic", pkt.Topic, "reason", err)
		metrics.RateLimitRejected("client", err.Error())
		return
	}

	metrics.MessagePublished(fh.QoS)

	if fh.Retain {
		c.broker.retainStore.Set(pkt.Topic, pkt.Payload, fh.QoS)
	}

	if c.broker.interceptors != nil {
		evt := &plugin.PublishEvent{
			ClientID: c.clientID,
			Topic:    pkt.Topic,
			Payload:  pkt.Payload,
			QoS:      fh.QoS,
			Retain:   fh.Retain,
		}
		if err := c.broker.interceptors.OnPublish(context.Background(), evt); err != nil {
			slog.Debug("publish rejected by interceptor", "client", c.clientID, "topic", pkt.Topic, "error", err)
			return
		}
		c.broker.routeMessage(evt.Topic, evt.Payload, evt.QoS, evt.Retain, false)
	} else {
		c.broker.routeMessage(pkt.Topic, pkt.Payload, fh.QoS, fh.Retain, false)
	}
}

func (c *Client) handlePuback(data []byte) {
	pkt, err := codec.DecodePubackPacket(data, 4)
	if err != nil {
		slog.Error("decode PUBACK error", "client", c.clientID, "error", err)
		return
	}
	c.inflight.Remove(pkt.PacketID)
}

func (c *Client) handlePubrec(data []byte) {
	pkt, err := codec.DecodePubrecPacket(data, 4)
	if err != nil {
		slog.Error("decode PUBREC error", "client", c.clientID, "error", err)
		return
	}
	c.inflight.Remove(pkt.PacketID)
	pubrel := &codec.PubrelPacket{PacketID: pkt.PacketID}
	c.send(pubrel.Encode())
}

func (c *Client) handlePubrel(data []byte) {
	pkt, err := codec.DecodePubrelPacket(data, 4)
	if err != nil {
		slog.Error("decode PUBREL error", "client", c.clientID, "error", err)
		return
	}
	c.broker.dedupStore.Remove(c.clientID, pkt.PacketID)
	pubcomp := &codec.PubcompPacket{PacketID: pkt.PacketID}
	c.send(pubcomp.Encode())
}

func (c *Client) handlePubcomp(data []byte) {
	_, _ = codec.DecodePubcompPacket(data, 4)
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
	pkt, err := codec.DecodeSubscribePacket(data, c.protocolVersion)
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

		if !c.broker.authorizer.Authorize(c.username, sub.TopicFilter, "subscribe") {
			metrics.ACLDenial("subscribe")
			returnCodes[i] = 0x80
			continue
		}

		if err := c.broker.rateLimiter.AllowSubscribe(c.clientID, sub.TopicFilter); err != nil {
			slog.Debug("subscribe rate limited", "client", c.clientID, "filter", sub.TopicFilter, "reason", err)
			metrics.RateLimitRejected("subscribe", err.Error())
			returnCodes[i] = 0x80
			continue
		}

		grantedQoS := sub.QoS
		if grantedQoS > 2 {
			grantedQoS = 2
		}

		if c.broker.interceptors != nil {
			evt := &plugin.SubscribeEvent{
				ClientID:    c.clientID,
				TopicFilter: sub.TopicFilter,
				QoS:         grantedQoS,
			}
			if err := c.broker.interceptors.OnSubscribe(context.Background(), evt); err != nil {
				slog.Debug("subscribe rejected by interceptor", "client", c.clientID, "filter", sub.TopicFilter, "error", err)
				returnCodes[i] = 0x80
				continue
			}
			grantedQoS = evt.QoS
			sub.TopicFilter = evt.TopicFilter // apply interceptor mutation
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
	pkt, err := codec.DecodeUnsubscribePacket(data, c.protocolVersion)
	if err != nil {
		slog.Error("decode UNSUBSCRIBE error", "client", c.clientID, "error", err)
		return
	}

	session := c.broker.sessions.Get(c.clientID)
	for i, filter := range pkt.TopicFilters {
		if c.broker.interceptors != nil {
			evt := &plugin.UnsubscribeEvent{
				ClientID:    c.clientID,
				TopicFilter: filter,
			}
			if err := c.broker.interceptors.OnUnsubscribe(context.Background(), evt); err != nil {
				slog.Debug("unsubscribe rejected by interceptor", "client", c.clientID, "filter", filter, "error", err)
				continue
			}
			pkt.TopicFilters[i] = evt.TopicFilter
			filter = evt.TopicFilter
		}
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

		if c.broker.interceptors != nil {
			reason := "error"
			if c.will == nil {
				reason = "clean"
			}
			c.broker.interceptors.OnDisconnect(&plugin.DisconnectEvent{
				ClientID:   c.clientID,
				Reason:     reason,
				RemoteAddr: c.conn.RemoteAddr().String(),
			})
		}

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
