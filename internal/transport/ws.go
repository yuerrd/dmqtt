package transport

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn wraps a *websocket.Conn to implement net.Conn.
// It converts between WebSocket binary frames and a byte-stream interface.
type wsConn struct {
	ws     *websocket.Conn
	reader io.Reader
	mu     sync.Mutex
}

func newWSConn(ws *websocket.Conn) *wsConn {
	return &wsConn{ws: ws}
}

func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.reader != nil {
			n, err := c.reader.Read(p)
			if err == io.EOF {
				c.reader = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}

		msgType, reader, err := c.ws.NextReader()
		if err != nil {
			return 0, err
		}
		if msgType != websocket.BinaryMessage {
			// Skip non-binary frames (text, etc.)
			continue
		}
		c.reader = reader
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Close() error {
	return c.ws.Close()
}

func (c *wsConn) LocalAddr() net.Addr {
	return c.ws.LocalAddr()
}

func (c *wsConn) RemoteAddr() net.Addr {
	return c.ws.RemoteAddr()
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.ws.SetReadDeadline(t)
}

func (c *wsConn) SetWriteDeadline(t time.Time) error {
	return c.ws.SetWriteDeadline(t)
}

// WSListener implements the Listener interface for MQTT over WebSocket.
type WSListener struct {
	server    *http.Server
	connCh    chan net.Conn
	listener  net.Listener
	upgrader  websocket.Upgrader
	done      chan struct{}
	closeOnce sync.Once
}

// NewWSListener creates a WebSocket listener on the given address.
// If tlsConfig is non-nil, the listener serves WSS (WebSocket over TLS).
func NewWSListener(addr string, tlsConfig *tls.Config) (*WSListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig)
	}

	w := &WSListener{
		connCh:   make(chan net.Conn, 256),
		listener: ln,
		upgrader: websocket.Upgrader{
			Subprotocols:    []string{"mqtt"},
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				// Allow requests with no origin (non-browser clients)
				if origin == "" {
					return true
				}
				// Parse origin and compare host exactly to prevent subdomain bypass
				u, err := url.Parse(origin)
				if err != nil {
					slog.Warn("WebSocket origin parse failed", "origin", origin, "error", err)
					return false
				}
				if u.Host == r.Host {
					return true
				}
				slog.Warn("WebSocket origin rejected", "origin", origin, "host", r.Host)
				return false
			},
		},
		done: make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/mqtt", w.handleWebSocket)

	w.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := w.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("WebSocket server error", "error", err)
		}
	}()

	return w, nil
}

func (w *WSListener) handleWebSocket(rw http.ResponseWriter, r *http.Request) {
	// Reject if client didn't request mqtt subprotocol
	hasMQTT := false
	for _, p := range websocket.Subprotocols(r) {
		if p == "mqtt" {
			hasMQTT = true
			break
		}
	}
	if !hasMQTT {
		http.Error(rw, "missing mqtt subprotocol", http.StatusBadRequest)
		return
	}

	ws, err := w.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		slog.Warn("WebSocket upgrade failed", "error", err, "remote", r.RemoteAddr)
		return
	}

	conn := newWSConn(ws)
	select {
	case w.connCh <- conn:
	case <-w.done:
		ws.Close()
	}
}

func (w *WSListener) Accept() (net.Conn, error) {
	select {
	case conn := <-w.connCh:
		return conn, nil
	case <-w.done:
		return nil, net.ErrClosed
	}
}

func (w *WSListener) Addr() string {
	return w.listener.Addr().String()
}

func (w *WSListener) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = w.server.Shutdown(ctx)
	})
	return err
}
