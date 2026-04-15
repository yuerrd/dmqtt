package codec

import (
	"bytes"
	"testing"
)

func TestDecodePublishQoS0(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "test/topic")
	buf.WriteString("hello world")

	pkt, err := DecodePublishPacket(buf.Bytes(), 0)
	if err != nil {
		t.Fatalf("DecodePublishPacket() error: %v", err)
	}
	if pkt.Topic != "test/topic" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "test/topic")
	}
	if string(pkt.Payload) != "hello world" {
		t.Errorf("Payload = %q, want %q", pkt.Payload, "hello world")
	}
	if pkt.PacketID != 0 {
		t.Errorf("PacketID = %d, want 0 for QoS 0", pkt.PacketID)
	}
}

func TestDecodePublishQoS1(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "sensor/temp")
	buf.Write([]byte{0x00, 0x0A}) // Packet ID = 10
	buf.WriteString(`{"temp":25.5}`)

	pkt, err := DecodePublishPacket(buf.Bytes(), 1)
	if err != nil {
		t.Fatalf("DecodePublishPacket() error: %v", err)
	}
	if pkt.Topic != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "sensor/temp")
	}
	if pkt.PacketID != 10 {
		t.Errorf("PacketID = %d, want 10", pkt.PacketID)
	}
	if string(pkt.Payload) != `{"temp":25.5}` {
		t.Errorf("Payload = %q, want %q", pkt.Payload, `{"temp":25.5}`)
	}
}

func TestPublishPacketEncode(t *testing.T) {
	pkt := &PublishPacket{
		Topic:    "a/b",
		PacketID: 0,
		QoS:      0,
		Dup:      false,
		Retain:   false,
		Payload:  []byte("hi"),
	}
	data := pkt.Encode()

	r := bytes.NewReader(data)
	fh, err := DecodeFixedHeader(r)
	if err != nil {
		t.Fatalf("DecodeFixedHeader error: %v", err)
	}
	if fh.PacketType != PUBLISH {
		t.Errorf("PacketType = %d, want %d", fh.PacketType, PUBLISH)
	}
	if fh.QoS != 0 {
		t.Errorf("QoS = %d, want 0", fh.QoS)
	}

	remaining := make([]byte, fh.RemainingLength)
	r.Read(remaining)

	decoded, err := DecodePublishPacket(remaining, fh.QoS)
	if err != nil {
		t.Fatalf("DecodePublishPacket error: %v", err)
	}
	if decoded.Topic != "a/b" {
		t.Errorf("Topic = %q, want %q", decoded.Topic, "a/b")
	}
	if string(decoded.Payload) != "hi" {
		t.Errorf("Payload = %q, want %q", decoded.Payload, "hi")
	}
}

func TestPubackEncodeDecode(t *testing.T) {
	pkt := &PubackPacket{PacketID: 42}
	data := pkt.Encode()

	expected := []byte{0x40, 0x02, 0x00, 0x2A}
	if !bytes.Equal(data, expected) {
		t.Errorf("PubackPacket.Encode() = %v, want %v", data, expected)
	}

	decoded, err := DecodePubackPacket(data[2:])
	if err != nil {
		t.Fatalf("DecodePubackPacket error: %v", err)
	}
	if decoded.PacketID != 42 {
		t.Errorf("PacketID = %d, want 42", decoded.PacketID)
	}
}
