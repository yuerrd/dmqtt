package transport

import (
	"net"
	"testing"
	"time"
)

func TestTCPListener_StartStop(t *testing.T) {
	l, err := NewTCPListener("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewTCPListener error: %v", err)
	}

	addr := l.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty string")
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	conn.Close()

	accepted, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}
	accepted.Close()

	err = l.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}
}
