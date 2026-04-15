package codec

import (
	"bytes"
	"testing"
)

func TestDecodeUnsubscribePacket(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x05})
	writeUTF8String(&buf, "sensor/+")

	pkt, err := DecodeUnsubscribePacket(buf.Bytes())
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

func TestUnsubackEncode(t *testing.T) {
	pkt := &UnsubackPacket{PacketID: 5}
	data := pkt.Encode()
	expected := []byte{0xB0, 0x02, 0x00, 0x05}
	if !bytes.Equal(data, expected) {
		t.Errorf("UnsubackPacket.Encode() = %v, want %v", data, expected)
	}
}
