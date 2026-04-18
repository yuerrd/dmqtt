package codec

import (
	"bytes"
	"testing"
)

func TestDecodeUnsubscribePacket(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x05})
	writeUTF8String(&buf, "sensor/+")

	pkt, err := DecodeUnsubscribePacket(buf.Bytes(), 4)
	if err != nil {
		t.Fatalf("DecodeUnsubscribePacket() error: %v", err)
	}
	if pkt.PacketID != 5 {
		t.Errorf("PacketID = %d, want 5", pkt.PacketID)
	}
	if len(pkt.TopicFilters) != 1 {
		t.Fatalf("TopicFilters count = %d, want 1", len(pkt.TopicFilters))
	}
	if pkt.TopicFilters[0] != "sensor/+" {
		t.Errorf("TopicFilters[0] = %q, want %q", pkt.TopicFilters[0], "sensor/+")
	}
}

func TestDecodeUnsubscribePacketV5(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x05})
	buf.WriteByte(0x00) // Empty properties
	writeUTF8String(&buf, "sensor/+")

	pkt, err := DecodeUnsubscribePacket(buf.Bytes(), 5)
	if err != nil {
		t.Fatalf("DecodeUnsubscribePacket() error: %v", err)
	}
	if pkt.PacketID != 5 {
		t.Errorf("PacketID = %d, want 5", pkt.PacketID)
	}
}

func TestUnsubackEncode(t *testing.T) {
	pkt := &UnsubackPacket{PacketID: 5}
	data := pkt.Encode()
	expected := []byte{0xB0, 0x02, 0x00, 0x05}
	if !bytes.Equal(data, expected) {
		t.Errorf("UnsubackPacket.Encode() = %v, want %v", data, expected)
	}
}

func TestUnsubackEncodeV5(t *testing.T) {
	pkt := &UnsubackPacket{
		PacketID:        5,
		ReasonCodes:     []byte{0x00, 0x11},
		ProtocolVersion: 5,
	}
	data := pkt.Encode()
	if data[0] != 0xB0 {
		t.Errorf("first byte = 0x%02X, want 0xB0", data[0])
	}
}

func TestUnsubackEncodeV31Unchanged(t *testing.T) {
	pkt := &UnsubackPacket{PacketID: 5}
	data := pkt.Encode()
	expected := []byte{0xB0, 0x02, 0x00, 0x05}
	if !bytes.Equal(data, expected) {
		t.Errorf("v3.1.1 UnsubackPacket.Encode() = %v, want %v", data, expected)
	}
}
