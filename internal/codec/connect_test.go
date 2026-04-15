package codec

import (
	"bytes"
	"testing"
)

func TestDecodeConnectPacket(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "MQTT")
	buf.WriteByte(0x04)
	buf.WriteByte(0x02) // Clean Session
	buf.Write([]byte{0x00, 0x3C}) // Keep Alive 60
	writeUTF8String(&buf, "test-client")

	pkt, err := DecodeConnectPacket(buf.Bytes())
	if err != nil {
		t.Fatalf("DecodeConnectPacket() error: %v", err)
	}
	if pkt.ProtocolName != "MQTT" {
		t.Errorf("ProtocolName = %q, want %q", pkt.ProtocolName, "MQTT")
	}
	if pkt.ProtocolLevel != 4 {
		t.Errorf("ProtocolLevel = %d, want %d", pkt.ProtocolLevel, 4)
	}
	if !pkt.CleanSession {
		t.Error("CleanSession = false, want true")
	}
	if pkt.KeepAlive != 60 {
		t.Errorf("KeepAlive = %d, want %d", pkt.KeepAlive, 60)
	}
	if pkt.ClientID != "test-client" {
		t.Errorf("ClientID = %q, want %q", pkt.ClientID, "test-client")
	}
}

func TestDecodeConnectPacketWithWill(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "MQTT")
	buf.WriteByte(0x04)
	buf.WriteByte(0x0E) // Clean Session=1, Will=1, Will QoS=1
	buf.Write([]byte{0x00, 0x3C})
	writeUTF8String(&buf, "client-with-will")
	writeUTF8String(&buf, "will/topic")
	writeBytes(&buf, []byte("will payload"))

	pkt, err := DecodeConnectPacket(buf.Bytes())
	if err != nil {
		t.Fatalf("DecodeConnectPacket() error: %v", err)
	}
	if !pkt.WillFlag {
		t.Error("WillFlag = false, want true")
	}
	if pkt.WillQoS != 1 {
		t.Errorf("WillQoS = %d, want %d", pkt.WillQoS, 1)
	}
	if pkt.WillTopic != "will/topic" {
		t.Errorf("WillTopic = %q, want %q", pkt.WillTopic, "will/topic")
	}
	if string(pkt.WillPayload) != "will payload" {
		t.Errorf("WillPayload = %q, want %q", pkt.WillPayload, "will payload")
	}
}

func TestDecodeConnectPacketWithUsernamePassword(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8String(&buf, "MQTT")
	buf.WriteByte(0x04)
	buf.WriteByte(0xC2) // Clean Session=1, Username=1, Password=1
	buf.Write([]byte{0x00, 0x3C})
	writeUTF8String(&buf, "auth-client")
	writeUTF8String(&buf, "admin")
	writeBytes(&buf, []byte("secret"))

	pkt, err := DecodeConnectPacket(buf.Bytes())
	if err != nil {
		t.Fatalf("DecodeConnectPacket() error: %v", err)
	}
	if pkt.Username != "admin" {
		t.Errorf("Username = %q, want %q", pkt.Username, "admin")
	}
	if string(pkt.Password) != "secret" {
		t.Errorf("Password = %q, want %q", pkt.Password, "secret")
	}
}

func TestConnackEncode(t *testing.T) {
	pkt := &ConnackPacket{
		SessionPresent: false,
		ReturnCode:     ConnackAccepted,
	}
	data := pkt.Encode()
	expected := []byte{0x20, 0x02, 0x00, 0x00}
	if !bytes.Equal(data, expected) {
		t.Errorf("ConnackPacket.Encode() = %v, want %v", data, expected)
	}
}

func TestConnackEncodeRefused(t *testing.T) {
	pkt := &ConnackPacket{
		SessionPresent: true,
		ReturnCode:     ConnackBadUsernameOrPassword,
	}
	data := pkt.Encode()
	expected := []byte{0x20, 0x02, 0x01, 0x04}
	if !bytes.Equal(data, expected) {
		t.Errorf("ConnackPacket.Encode() = %v, want %v", data, expected)
	}
}

// Helper: write a UTF-8 encoded string (2-byte length prefix + string)
func writeUTF8String(buf *bytes.Buffer, s string) {
	buf.WriteByte(byte(len(s) >> 8))
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
}

// Helper: write binary data (2-byte length prefix + data)
func writeBytes(buf *bytes.Buffer, data []byte) {
	buf.WriteByte(byte(len(data) >> 8))
	buf.WriteByte(byte(len(data)))
	buf.Write(data)
}
