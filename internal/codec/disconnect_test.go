package codec

import (
	"bytes"
	"testing"
)

func TestDisconnectDetection(t *testing.T) {
	data := []byte{0xE0, 0x00}
	r := bytes.NewReader(data)
	fh, err := DecodeFixedHeader(r)
	if err != nil {
		t.Fatalf("DecodeFixedHeader error: %v", err)
	}
	if fh.PacketType != DISCONNECT {
		t.Errorf("PacketType = %d, want %d", fh.PacketType, DISCONNECT)
	}
	if fh.RemainingLength != 0 {
		t.Errorf("RemainingLength = %d, want 0", fh.RemainingLength)
	}
}
