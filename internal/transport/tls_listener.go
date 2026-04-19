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
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
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
