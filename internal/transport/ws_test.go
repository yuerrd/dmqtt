package transport

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// testUpgrader is a websocket.Upgrader for tests.
var testUpgrader = websocket.Upgrader{
	Subprotocols: []string{"mqtt"},
	CheckOrigin:  func(r *http.Request) bool { return true },
}

func TestWsConn_ReadWrite(t *testing.T) {
	connCh := make(chan net.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		connCh <- newWSConn(ws)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/mqtt"
	clientWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientWS.Close()

	var srvConn net.Conn
	select {
	case srvConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server connection")
	}
	defer srvConn.Close()

	// Client writes binary frame → server reads via wsConn.Read
	payload := []byte("hello mqtt")
	if err := clientWS.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("client write error: %v", err)
	}

	buf := make([]byte, 64)
	n, err := srvConn.Read(buf)
	if err != nil {
		t.Fatalf("wsConn Read error: %v", err)
	}
	if string(buf[:n]) != "hello mqtt" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "hello mqtt")
	}

	// Server writes via wsConn.Write → client reads binary frame
	if _, err := srvConn.Write([]byte("hello back")); err != nil {
		t.Fatalf("wsConn Write error: %v", err)
	}

	_, msg, err := clientWS.ReadMessage()
	if err != nil {
		t.Fatalf("client read error: %v", err)
	}
	if string(msg) != "hello back" {
		t.Fatalf("got %q, want %q", string(msg), "hello back")
	}
}

func TestWsConn_PartialRead(t *testing.T) {
	connCh := make(chan net.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- newWSConn(ws)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/mqtt"
	clientWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientWS.Close()

	var srvConn net.Conn
	select {
	case srvConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	defer srvConn.Close()

	// Write a 10-byte message, then read in 3-byte chunks
	if err := clientWS.WriteMessage(websocket.BinaryMessage, []byte("0123456789")); err != nil {
		t.Fatalf("write error: %v", err)
	}

	var got []byte
	buf := make([]byte, 3)
	for len(got) < 10 {
		n, err := srvConn.Read(buf)
		if err != nil {
			t.Fatalf("read error after %d bytes: %v", len(got), err)
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != "0123456789" {
		t.Fatalf("got %q, want %q", string(got), "0123456789")
	}
}

func TestWsConn_CloseReturnsEOF(t *testing.T) {
	connCh := make(chan net.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- newWSConn(ws)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/mqtt"
	clientWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	var srvConn net.Conn
	select {
	case srvConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Client closes the WebSocket
	clientWS.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	clientWS.Close()

	// Server read should eventually return error
	buf := make([]byte, 64)
	srvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = srvConn.Read(buf)
	if err == nil {
		t.Fatal("expected error after client close, got nil")
	}
}

func TestWsConn_Addrs(t *testing.T) {
	connCh := make(chan net.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- newWSConn(ws)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/mqtt"
	clientWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientWS.Close()

	var srvConn net.Conn
	select {
	case srvConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	defer srvConn.Close()

	if srvConn.LocalAddr() == nil {
		t.Error("LocalAddr() returned nil")
	}
	if srvConn.RemoteAddr() == nil {
		t.Error("RemoteAddr() returned nil")
	}
}

// --- WSListener tests ---

func TestWSListener_Accept(t *testing.T) {
	ln, err := NewWSListener("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewWSListener error: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty")
	}

	// Connect a WebSocket client with mqtt subprotocol
	wsURL := "ws://" + addr + "/mqtt"
	dialer := websocket.Dialer{}
	clientWS, resp, err := dialer.Dial(wsURL, http.Header{
		"Sec-WebSocket-Protocol": []string{"mqtt"},
	})
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientWS.Close()

	if resp.Header.Get("Sec-WebSocket-Protocol") != "mqtt" {
		t.Fatalf("subprotocol = %q, want %q", resp.Header.Get("Sec-WebSocket-Protocol"), "mqtt")
	}

	// Accept should return a connection
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}
	defer conn.Close()

	// Verify data flows through
	if err := clientWS.WriteMessage(websocket.BinaryMessage, []byte("test")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(buf[:n]) != "test" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "test")
	}
}

func TestWSListener_RejectsNoSubprotocol(t *testing.T) {
	ln, err := NewWSListener("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewWSListener error: %v", err)
	}
	defer ln.Close()

	// Connect WITHOUT mqtt subprotocol
	wsURL := "ws://" + ln.Addr() + "/mqtt"
	dialer := websocket.Dialer{}
	_, resp, err := dialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected dial error without mqtt subprotocol")
	}
	if resp != nil && resp.StatusCode != http.StatusBadRequest {
		t.Logf("status = %d (connection rejected as expected)", resp.StatusCode)
	}
}

func TestWSListener_Close(t *testing.T) {
	ln, err := NewWSListener("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewWSListener error: %v", err)
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Accept after close should return error
	_, err = ln.Accept()
	if err == nil {
		t.Fatal("expected error from Accept after Close")
	}
}

func TestWSListener_NonMQTTPath(t *testing.T) {
	ln, err := NewWSListener("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewWSListener error: %v", err)
	}
	defer ln.Close()

	// HTTP GET to a non-/mqtt path should return 404
	resp, err := http.Get("http://" + ln.Addr() + "/other")
	if err != nil {
		t.Fatalf("HTTP GET error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

// --- Integration test: MQTT CONNECT over WebSocket ---

func TestWSListener_MQTTConnect(t *testing.T) {
	ln, err := NewWSListener("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewWSListener error: %v", err)
	}
	defer ln.Close()

	// Build a minimal MQTT CONNECT packet (MQTT 3.1.1)
	connectPacket := buildMQTTConnectPacket("ws-test-client")

	// Send CONNECT via WebSocket
	wsURL := "ws://" + ln.Addr() + "/mqtt"
	dialer := websocket.Dialer{}
	clientWS, _, err := dialer.Dial(wsURL, http.Header{
		"Sec-WebSocket-Protocol": []string{"mqtt"},
	})
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientWS.Close()

	if err := clientWS.WriteMessage(websocket.BinaryMessage, connectPacket); err != nil {
		t.Fatalf("write CONNECT error: %v", err)
	}

	// Accept the connection and read the CONNECT packet back
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	// Verify it's a CONNECT packet (first byte: 0x10 = CONNECT type << 4)
	if buf[0] != 0x10 {
		t.Fatalf("first byte = 0x%02x, want 0x10 (CONNECT)", buf[0])
	}
	if n != len(connectPacket) {
		t.Fatalf("read %d bytes, want %d", n, len(connectPacket))
	}

	// Send a CONNACK back via wsConn → client reads it
	connack := []byte{0x20, 0x02, 0x00, 0x00} // CONNACK, accepted
	if _, err := conn.Write(connack); err != nil {
		t.Fatalf("write CONNACK error: %v", err)
	}

	_, msg, err := clientWS.ReadMessage()
	if err != nil {
		t.Fatalf("client read CONNACK error: %v", err)
	}
	if len(msg) != 4 || msg[0] != 0x20 {
		t.Fatalf("CONNACK = %x, want 20020000", msg)
	}
}

// buildMQTTConnectPacket constructs a minimal MQTT 3.1.1 CONNECT packet.
func buildMQTTConnectPacket(clientID string) []byte {
	// Variable header
	varHeader := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', // Protocol Name
		0x04,       // Protocol Level (4 = MQTT 3.1.1)
		0x02,       // Connect Flags (Clean Session)
		0x00, 0x3C, // Keep Alive (60 seconds)
	}

	// Payload: Client ID (length-prefixed UTF-8 string)
	clientIDBytes := []byte(clientID)
	payload := make([]byte, 2+len(clientIDBytes))
	payload[0] = byte(len(clientIDBytes) >> 8)
	payload[1] = byte(len(clientIDBytes))
	copy(payload[2:], clientIDBytes)

	// Remaining length
	remainingLength := len(varHeader) + len(payload)

	// Fixed header: packet type (0x10) + remaining length
	fixedHeader := []byte{0x10, byte(remainingLength)}

	pkt := make([]byte, 0, len(fixedHeader)+remainingLength)
	pkt = append(pkt, fixedHeader...)
	pkt = append(pkt, varHeader...)
	pkt = append(pkt, payload...)
	return pkt
}
