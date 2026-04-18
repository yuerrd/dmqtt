package codec

import (
	"bytes"
	"testing"
)

func TestDecodeSubscribePacket(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x01}) // Packet ID = 1
	writeUTF8String(&buf, "sensor/+")
	buf.WriteByte(0x01)
	writeUTF8String(&buf, "control/#")
	buf.WriteByte(0x00)

	pkt, err := DecodeSubscribePacket(buf.Bytes(), 4)
	if err != nil {
		t.Fatalf("DecodeSubscribePacket() error: %v", err)
	}
	if pkt.PacketID != 1 {
		t.Errorf("PacketID = %d, want 1", pkt.PacketID)
	}
	if len(pkt.Subscriptions) != 2 {
		t.Fatalf("Subscriptions count = %d, want 2", len(pkt.Subscriptions))
	}
	if pkt.Subscriptions[0].TopicFilter != "sensor/+" {
		t.Errorf("Subscriptions[0].TopicFilter = %q, want %q", pkt.Subscriptions[0].TopicFilter, "sensor/+")
	}
	if pkt.Subscriptions[0].QoS != 1 {
		t.Errorf("Subscriptions[0].QoS = %d, want 1", pkt.Subscriptions[0].QoS)
	}
	if pkt.Subscriptions[1].TopicFilter != "control/#" {
		t.Errorf("Subscriptions[1].TopicFilter = %q, want %q", pkt.Subscriptions[1].TopicFilter, "control/#")
	}
}

func TestDecodeSubscribePacketV5(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x01})
	buf.WriteByte(0x00) // Empty properties
	writeUTF8String(&buf, "sensor/+")
	buf.WriteByte(0x01)

	pkt, err := DecodeSubscribePacket(buf.Bytes(), 5)
	if err != nil {
		t.Fatalf("DecodeSubscribePacket() error: %v", err)
	}
	if pkt.PacketID != 1 {
		t.Errorf("PacketID = %d, want 1", pkt.PacketID)
	}
	if pkt.Properties == nil {
		t.Error("Properties is nil for v5")
	}
	if len(pkt.Subscriptions) != 1 || pkt.Subscriptions[0].TopicFilter != "sensor/+" {
		t.Errorf("Subscriptions = %+v, want [{sensor/+ 1}]", pkt.Subscriptions)
	}
}

func TestDecodeSubscribePacketV31Unchanged(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x01})
	writeUTF8String(&buf, "sensor/+")
	buf.WriteByte(0x01)
	writeUTF8String(&buf, "control/#")
	buf.WriteByte(0x00)

	pkt, err := DecodeSubscribePacket(buf.Bytes(), 4)
	if err != nil {
		t.Fatalf("DecodeSubscribePacket() error: %v", err)
	}
	if len(pkt.Subscriptions) != 2 {
		t.Fatalf("Subscriptions count = %d, want 2", len(pkt.Subscriptions))
	}
}

func TestSubackEncode(t *testing.T) {
	pkt := &SubackPacket{
		PacketID:    1,
		ReturnCodes: []byte{0x01, 0x00},
	}
	data := pkt.Encode()
	expected := []byte{0x90, 0x04, 0x00, 0x01, 0x01, 0x00}
	if !bytes.Equal(data, expected) {
		t.Errorf("SubackPacket.Encode() = %v, want %v", data, expected)
	}
}

func TestSubackEncodeV5(t *testing.T) {
	reason := "granted"
	pkt := &SubackPacket{
		PacketID:        1,
		ReturnCodes:     []byte{0x01, 0x00},
		Properties:      &Properties{ReasonString: &reason},
		ProtocolVersion: 5,
	}
	data := pkt.Encode()
	if data[0] != 0x90 {
		t.Errorf("first byte = 0x%02X, want 0x90", data[0])
	}
}
