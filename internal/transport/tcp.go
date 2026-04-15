package transport

import "net"

// TCPListener wraps a standard net.Listener for MQTT over TCP.
type TCPListener struct {
	listener net.Listener
}

func NewTCPListener(addr string) (*TCPListener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCPListener{listener: l}, nil
}

func (t *TCPListener) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *TCPListener) Addr() string {
	return t.listener.Addr().String()
}

func (t *TCPListener) Close() error {
	return t.listener.Close()
}
