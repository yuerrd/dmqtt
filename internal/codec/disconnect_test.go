package codec

import (
	"bytes"
	"testing"
)

func TestDisconnectEncodeV5(t *testing.T) {
	pkt := &DisconnectPacket{
		ReasonCode:      DisconnSessionTakenOver,
		ProtocolVersion: 5,
	}
	data := pkt.Encode()
	if data[0] != 0xE0 {
		t.Errorf("first byte = 0x%02X, want 0xE0", data[0])
	}
	if data[1] != 0x02 {
		t.Errorf("remaining length = %d, want 2", data[1])
	}
	if data[2] != DisconnSessionTakenOver {
		t.Errorf("reason code = 0x%02X, want 0x%02X", data[2], DisconnSessionTakenOver)
	}
}

func TestDisconnectEncodeV31(t *testing.T) {
	pkt := &DisconnectPacket{ProtocolVersion: 4}
	data := pkt.Encode()
	expected := []byte{0xE0, 0x00}
	if !bytes.Equal(data, expected) {
		t.Errorf("v3.1.1 DisconnectPacket.Encode() = %v, want %v", data, expected)
	}
}

func TestDecodeDisconnectPacketV5(t *testing.T) {
	data := []byte{DisconnSessionTakenOver, 0x00}
	pkt, err := DecodeDisconnectPacket(data)
	if err != nil {
		t.Fatalf("DecodeDisconnectPacket error: %v", err)
	}
	if pkt.ReasonCode != DisconnSessionTakenOver {
		t.Errorf("ReasonCode = 0x%02X, want 0x%02X", pkt.ReasonCode, DisconnSessionTakenOver)
	}
}

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
