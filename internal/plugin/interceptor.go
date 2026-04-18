package plugin

import "context"

// Interceptor is the base interface for all interceptors.
type Interceptor interface {
	Name() string
	Init() error
	Close() error
}

// --- Interceptable hooks (return error to reject) ---

// OnConnectInterceptor is called after authentication passes, before CONNACK.
type OnConnectInterceptor interface {
	OnConnect(ctx context.Context, evt *ConnectEvent) error
}

// OnPublishInterceptor is called before message routing.
type OnPublishInterceptor interface {
	OnPublish(ctx context.Context, evt *PublishEvent) error
}

// OnSubscribeInterceptor is called before subscription is written.
type OnSubscribeInterceptor interface {
	OnSubscribe(ctx context.Context, evt *SubscribeEvent) error
}

// OnDeliveryInterceptor is called before delivering to each subscriber.
type OnDeliveryInterceptor interface {
	OnDelivery(ctx context.Context, evt *DeliveryEvent) error
}

// --- Notification hooks (async, no return) ---

// OnDisconnectInterceptor is called after client disconnects.
type OnDisconnectInterceptor interface {
	OnDisconnect(evt *DisconnectEvent)
}

// OnSessionExpiredInterceptor is called when a session expires.
type OnSessionExpiredInterceptor interface {
	OnSessionExpired(evt *SessionExpiredEvent)
}

// ConnectEvent holds data for OnConnect hook.
type ConnectEvent struct {
	ClientID     string
	Username     string
	CleanSession bool // Value means CleanStart for v5
	RemoteAddr   string
}

// PublishEvent holds data for OnPublish hook. Topic and Payload are mutable.
type PublishEvent struct {
	ClientID string
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
}

// SubscribeEvent holds data for OnSubscribe hook. QoS is mutable.
type SubscribeEvent struct {
	ClientID    string
	TopicFilter string
	QoS         byte
}

// DeliveryEvent holds data for OnDelivery hook. Payload is mutable.
type DeliveryEvent struct {
	ClientID string
	Topic    string
	Payload  []byte
	QoS      byte
}

// DisconnectEvent holds data for OnDisconnect hook.
type DisconnectEvent struct {
	ClientID   string
	Reason     string
	RemoteAddr string
}

// SessionExpiredEvent holds data for OnSessionExpired hook.
type SessionExpiredEvent struct {
	ClientID string
}

// UnsubscribeEvent is fired when a client unsubscribes from a topic filter.
type UnsubscribeEvent struct {
	ClientID    string
	TopicFilter string // Mutable: interceptors may modify this (e.g., add prefix)
}

// OnUnsubscribeInterceptor is called when a client unsubscribes.
type OnUnsubscribeInterceptor interface {
	OnUnsubscribe(ctx context.Context, evt *UnsubscribeEvent) error
}
