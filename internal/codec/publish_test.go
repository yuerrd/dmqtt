package codec

import (
	"bytes"
	"testing"
)

func TestDecodePublishQoS0(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "test/topic")
	buf.WriteString("hello world")

	pkt, err := DecodePublishPacket(buf.Bytes(), 0, 4)
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

	pkt, err := DecodePublishPacket(buf.Bytes(), 1, 4)
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

	decoded, err := DecodePublishPacket(remaining, fh.QoS, 4)
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

	decoded, err := DecodePubackPacket(data[2:], 4)
	if err != nil {
		t.Fatalf("DecodePubackPacket error: %v", err)
	}
	if decoded.PacketID != 42 {
		t.Errorf("PacketID = %d, want 42", decoded.PacketID)
	}
}

func TestPubrecPacket_EncodeDecode(t *testing.T) {
	pkt := &PubrecPacket{PacketID: 42}
	data := pkt.Encode()

	// PUBREC is type 5 (0x50), with 2 bytes remaining
	if data[0] != 0x50 {
		t.Errorf("First byte = 0x%02X, want 0x50", data[0])
	}
	if data[1] != 0x02 {
		t.Errorf("Remaining length = %d, want 2", data[1])
	}

	decoded, err := DecodePubrecPacket(data[2:], 4)
	if err != nil {
		t.Fatalf("DecodePubrecPacket error: %v", err)
	}
	if decoded.PacketID != 42 {
		t.Errorf("PacketID = %d, want 42", decoded.PacketID)
	}
}

func TestPubrelPacket_EncodeDecode(t *testing.T) {
	pkt := &PubrelPacket{PacketID: 100}
	data := pkt.Encode()

	// PUBREL is type 6 (0x60) with flags 0x02, so first byte is 0x62
	if data[0] != 0x62 {
		t.Errorf("First byte = 0x%02X, want 0x62", data[0])
	}
	if data[1] != 0x02 {
		t.Errorf("Remaining length = %d, want 2", data[1])
	}

	decoded, err := DecodePubrelPacket(data[2:], 4)
	if err != nil {
		t.Fatalf("DecodePubrelPacket error: %v", err)
	}
	if decoded.PacketID != 100 {
		t.Errorf("PacketID = %d, want 100", decoded.PacketID)
	}
}

func TestPubcompPacket_EncodeDecode(t *testing.T) {
	pkt := &PubcompPacket{PacketID: 65535}
	data := pkt.Encode()

	// PUBCOMP is type 7 (0x70)
	if data[0] != 0x70 {
		t.Errorf("First byte = 0x%02X, want 0x70", data[0])
	}
	if data[1] != 0x02 {
		t.Errorf("Remaining length = %d, want 2", data[1])
	}

	decoded, err := DecodePubcompPacket(data[2:], 4)
	if err != nil {
		t.Fatalf("DecodePubcompPacket error: %v", err)
	}
	if decoded.PacketID != 65535 {
		t.Errorf("PacketID = %d, want 65535", decoded.PacketID)
	}
}

func TestDecodePublishQoS0_V31(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "test/topic")
	buf.WriteString("hello")

	pkt, err := DecodePublishPacket(buf.Bytes(), 0, 4)
	if err != nil {
		t.Fatalf("DecodePublishPacket() error: %v", err)
	}
	if pkt.Topic != "test/topic" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "test/topic")
	}
	if pkt.Properties != nil {
		t.Error("Properties should be nil for v3.1.1")
	}
}

func TestDecodePublishQoS1_V5(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "sensor/temp")
	buf.Write([]byte{0x00, 0x0A}) // Packet ID = 10
	// Properties: empty
	buf.WriteByte(0x00)
	buf.WriteString(`{"temp":25.5}`)

	pkt, err := DecodePublishPacket(buf.Bytes(), 1, 5)
	if err != nil {
		t.Fatalf("DecodePublishPacket() error: %v", err)
	}
	if pkt.Topic != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "sensor/temp")
	}
	if pkt.PacketID != 10 {
		t.Errorf("PacketID = %d, want 10", pkt.PacketID)
	}
	if pkt.Properties == nil {
		t.Fatal("Properties is nil for v5 PUBLISH")
	}
}

func TestPublishEncodeV5(t *testing.T) {
	contentType := "application/json"
	pkt := &PublishPacket{
		Topic:           "a/b",
		QoS:             0,
		Payload:         []byte("hi"),
		Properties:      &Properties{ContentType: &contentType},
		ProtocolVersion: 5,
	}
	data := pkt.Encode()

	r := bytes.NewReader(data)
	fh, err := DecodeFixedHeader(r)
	if err != nil {
		t.Fatalf("DecodeFixedHeader error: %v", err)
	}
	remaining := make([]byte, fh.RemainingLength)
	r.Read(remaining)

	decoded, err := DecodePublishPacket(remaining, fh.QoS, 5)
	if err != nil {
		t.Fatalf("DecodePublishPacket error: %v", err)
	}
	if decoded.Properties == nil || decoded.Properties.ContentType == nil {
		t.Fatal("ContentType not decoded")
	}
	if *decoded.Properties.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", *decoded.Properties.ContentType, "application/json")
	}
}

func TestPubackEncodeDecodeV5(t *testing.T) {
	pkt := &PubackPacket{
		PacketID:        42,
		ReasonCode:      ReasonCodeSuccess,
		ProtocolVersion: 5,
	}
	data := pkt.Encode()
	// v5 with success + no properties → same as v3.1.1 (just PacketID)
	expected := []byte{0x40, 0x02, 0x00, 0x2A}
	if !bytes.Equal(data, expected) {
		t.Errorf("v5 PUBACK success = %v, want %v", data, expected)
	}
}

func TestPubackEncodeDecodeV5WithReason(t *testing.T) {
	reason := "quota exceeded"
	pkt := &PubackPacket{
		PacketID:        42,
		ReasonCode:      ReasonCodeQuotaExceeded,
		Properties:      &Properties{ReasonString: &reason},
		ProtocolVersion: 5,
	}
	data := pkt.Encode()

	decoded, err := DecodePubackPacket(data[2:], 5) // skip fixed header
	if err != nil {
		t.Fatalf("DecodePubackPacket error: %v", err)
	}
	if decoded.PacketID != 42 {
		t.Errorf("PacketID = %d, want 42", decoded.PacketID)
	}
	if decoded.ReasonCode != ReasonCodeQuotaExceeded {
		t.Errorf("ReasonCode = 0x%02X, want 0x%02X", decoded.ReasonCode, ReasonCodeQuotaExceeded)
	}
	if decoded.Properties == nil || decoded.Properties.ReasonString == nil {
		t.Fatal("ReasonString not decoded")
	}
}

func TestPubackEncodeV31Unchanged(t *testing.T) {
	pkt := &PubackPacket{PacketID: 42}
	data := pkt.Encode()
	expected := []byte{0x40, 0x02, 0x00, 0x2A}
	if !bytes.Equal(data, expected) {
		t.Errorf("v3.1.1 PubackPacket.Encode() = %v, want %v", data, expected)
	}
}
