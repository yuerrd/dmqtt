package transport

import "net"

// Listener abstracts a network listener (TCP, QUIC, WebSocket).
type Listener interface {
	Accept() (net.Conn, error)
	Addr() string
	Close() error
}
