package codec

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeEmptyProperties(t *testing.T) {
	p := &Properties{}
	data := p.Encode()
	// Empty properties = single byte 0x00 (length = 0)
	if len(data) != 1 || data[0] != 0x00 {
		t.Fatalf("empty Properties.Encode() = %v, want [0x00]", data)
	}

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	if propLen != 0 {
		t.Fatalf("property length = %d, want 0", propLen)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.SessionExpiryInterval != nil {
		t.Error("SessionExpiryInterval should be nil for empty properties")
	}
}

func TestEncodeDecodeSessionExpiryInterval(t *testing.T) {
	val := uint32(300)
	p := &Properties{SessionExpiryInterval: &val}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.SessionExpiryInterval == nil || *decoded.SessionExpiryInterval != 300 {
		t.Errorf("SessionExpiryInterval = %v, want 300", decoded.SessionExpiryInterval)
	}
}

func TestEncodeDecodeReceiveMaximum(t *testing.T) {
	val := uint16(50)
	p := &Properties{ReceiveMaximum: &val}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.ReceiveMaximum == nil || *decoded.ReceiveMaximum != 50 {
		t.Errorf("ReceiveMaximum = %v, want 50", decoded.ReceiveMaximum)
	}
}

func TestEncodeDecodeReasonString(t *testing.T) {
	val := "connection refused"
	p := &Properties{ReasonString: &val}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.ReasonString == nil || *decoded.ReasonString != "connection refused" {
		t.Errorf("ReasonString = %v, want %q", decoded.ReasonString, "connection refused")
	}
}

func TestEncodeDecodeBinaryData(t *testing.T) {
	corrData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	p := &Properties{CorrelationData: corrData}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if !bytes.Equal(decoded.CorrelationData, corrData) {
		t.Errorf("CorrelationData = %v, want %v", decoded.CorrelationData, corrData)
	}
}

func TestEncodeDecodeByteProperty(t *testing.T) {
	val := byte(1)
	p := &Properties{PayloadFormatIndicator: &val}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.PayloadFormatIndicator == nil || *decoded.PayloadFormatIndicator != 1 {
		t.Errorf("PayloadFormatIndicator = %v, want 1", decoded.PayloadFormatIndicator)
	}
}

func TestEncodeDecodeUserProperties(t *testing.T) {
	p := &Properties{
		UserProperties: []UserProperty{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		},
	}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if len(decoded.UserProperties) != 2 {
		t.Fatalf("UserProperties count = %d, want 2", len(decoded.UserProperties))
	}
	if decoded.UserProperties[0].Key != "key1" || decoded.UserProperties[0].Value != "val1" {
		t.Errorf("UserProperties[0] = %+v, want {key1, val1}", decoded.UserProperties[0])
	}
	if decoded.UserProperties[1].Key != "key2" || decoded.UserProperties[1].Value != "val2" {
		t.Errorf("UserProperties[1] = %+v, want {key2, val2}", decoded.UserProperties[1])
	}
}

func TestEncodeDecodeMultipleProperties(t *testing.T) {
	expiry := uint32(3600)
	recvMax := uint16(100)
	reason := "test"
	p := &Properties{
		SessionExpiryInterval: &expiry,
		ReceiveMaximum:        &recvMax,
		ReasonString:          &reason,
	}
	data := p.Encode()

	propLen, n, err := decodeVarInt(data, 0)
	if err != nil {
		t.Fatalf("decodeVarInt error: %v", err)
	}
	decoded, err := DecodeProperties(data[n:], propLen)
	if err != nil {
		t.Fatalf("DecodeProperties error: %v", err)
	}
	if decoded.SessionExpiryInterval == nil || *decoded.SessionExpiryInterval != 3600 {
		t.Errorf("SessionExpiryInterval = %v, want 3600", decoded.SessionExpiryInterval)
	}
	if decoded.ReceiveMaximum == nil || *decoded.ReceiveMaximum != 100 {
		t.Errorf("ReceiveMaximum = %v, want 100", decoded.ReceiveMaximum)
	}
	if decoded.ReasonString == nil || *decoded.ReasonString != "test" {
		t.Errorf("ReasonString = %v, want %q", decoded.ReasonString, "test")
	}
}

func TestDecodePropertiesTruncated(t *testing.T) {
	// Property ID for SessionExpiryInterval (0x11) but no value bytes
	data := []byte{0x11}
	_, err := DecodeProperties(data, len(data))
	if err == nil {
		t.Fatal("expected error for truncated property, got nil")
	}
}

func TestEncodeDecodeNilProperties(t *testing.T) {
	var p *Properties
	data := p.Encode()
	if len(data) != 1 || data[0] != 0x00 {
		t.Fatalf("nil Properties.Encode() = %v, want [0x00]", data)
	}
}

func TestVarIntRoundTrip(t *testing.T) {
	tests := []int{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	for _, v := range tests {
		encoded := encodeVarInt(v)
		decoded, n, err := decodeVarInt(encoded, 0)
		if err != nil {
			t.Errorf("decodeVarInt(%d) error: %v", v, err)
			continue
		}
		if decoded != v {
			t.Errorf("decodeVarInt(encodeVarInt(%d)) = %d", v, decoded)
		}
		if n != len(encoded) {
			t.Errorf("decodeVarInt consumed %d bytes, encoded was %d", n, len(encoded))
		}
	}
}
