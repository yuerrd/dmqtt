package codec

import (
	"bytes"
	"testing"
)

func TestReadPacket_Connect(t *testing.T) {
	var payload bytes.Buffer
	writeUTF8String(&payload, "MQTT")
	payload.WriteByte(0x04)
	payload.WriteByte(0x02)
	payload.Write([]byte{0x00, 0x3C})
	writeUTF8String(&payload, "reader-test")

	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: payload.Len(),
	}

	var fullPacket bytes.Buffer
	fullPacket.Write(fh.Encode())
	fullPacket.Write(payload.Bytes())

	pktType, data, err := ReadPacket(&fullPacket)
	if err != nil {
		t.Fatalf("ReadPacket() error: %v", err)
	}
	if pktType.PacketType != CONNECT {
		t.Errorf("PacketType = %d, want %d", pktType.PacketType, CONNECT)
	}
	if len(data) != payload.Len() {
		t.Errorf("data length = %d, want %d", len(data), payload.Len())
	}
}

func TestReadPacket_Pingreq(t *testing.T) {
	packet := []byte{0xC0, 0x00}
	r := bytes.NewReader(packet)

	pktType, data, err := ReadPacket(r)
	if err != nil {
		t.Fatalf("ReadPacket() error: %v", err)
	}
	if pktType.PacketType != PINGREQ {
		t.Errorf("PacketType = %d, want %d", pktType.PacketType, PINGREQ)
	}
	if len(data) != 0 {
		t.Errorf("data length = %d, want 0", len(data))
	}
}
