package transport

import (
	"crypto/tls"
	"net"
)

// TLSListener wraps a TLS listener for MQTT over TLS.
type TLSListener struct {
	listener net.Listener
}

func NewTLSListener(addr, certFile, keyFile string) (*TLSListener, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	ln, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &TLSListener{listener: ln}, nil
}

func (t *TLSListener) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *TLSListener) Addr() string {
	return t.listener.Addr().String()
}

func (t *TLSListener) Close() error {
	return t.listener.Close()
}
