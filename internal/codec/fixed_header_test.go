package codec

import (
	"bytes"
	"testing"
)

func TestEncodeRemainingLength(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		expected []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"single byte max", 127, []byte{0x7F}},
		{"two bytes min", 128, []byte{0x80, 0x01}},
		{"two bytes 16383", 16383, []byte{0xFF, 0x7F}},
		{"three bytes min", 16384, []byte{0x80, 0x80, 0x01}},
		{"three bytes 2097151", 2097151, []byte{0xFF, 0xFF, 0x7F}},
		{"four bytes min", 2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{"four bytes max", 268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EncodeRemainingLength(tt.length)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("EncodeRemainingLength(%d) = %v, want %v", tt.length, result, tt.expected)
			}
		})
	}
}

func TestDecodeRemainingLength(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int
		wantErr  bool
	}{
		{"zero", []byte{0x00}, 0, false},
		{"127", []byte{0x7F}, 127, false},
		{"128", []byte{0x80, 0x01}, 128, false},
		{"16383", []byte{0xFF, 0x7F}, 16383, false},
		{"268435455", []byte{0xFF, 0xFF, 0xFF, 0x7F}, 268435455, false},
		{"too many bytes", []byte{0x80, 0x80, 0x80, 0x80, 0x01}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.input)
			result, err := DecodeRemainingLength(r)
			if tt.wantErr {
				if err == nil {
					t.Errorf("DecodeRemainingLength() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("DecodeRemainingLength() unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("DecodeRemainingLength() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestFixedHeaderEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		header FixedHeader
	}{
		{
			"CONNECT",
			FixedHeader{PacketType: CONNECT, RemainingLength: 50},
		},
		{
			"PUBLISH QoS 0",
			FixedHeader{PacketType: PUBLISH, QoS: 0, RemainingLength: 100},
		},
		{
			"PUBLISH QoS 1 DUP Retain",
			FixedHeader{PacketType: PUBLISH, Dup: true, QoS: 1, Retain: true, RemainingLength: 200},
		},
		{
			"SUBSCRIBE",
			FixedHeader{PacketType: SUBSCRIBE, QoS: 1, RemainingLength: 30},
		},
		{
			"PINGREQ",
			FixedHeader{PacketType: PINGREQ, RemainingLength: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.header.Encode()
			r := bytes.NewReader(encoded)
			decoded, err := DecodeFixedHeader(r)
			if err != nil {
				t.Fatalf("DecodeFixedHeader() error: %v", err)
			}
			if decoded.PacketType != tt.header.PacketType {
				t.Errorf("PacketType = %d, want %d", decoded.PacketType, tt.header.PacketType)
			}
			if decoded.Dup != tt.header.Dup {
				t.Errorf("Dup = %v, want %v", decoded.Dup, tt.header.Dup)
			}
			if decoded.QoS != tt.header.QoS {
				t.Errorf("QoS = %d, want %d", decoded.QoS, tt.header.QoS)
			}
			if decoded.Retain != tt.header.Retain {
				t.Errorf("Retain = %v, want %v", decoded.Retain, tt.header.Retain)
			}
			if decoded.RemainingLength != tt.header.RemainingLength {
				t.Errorf("RemainingLength = %d, want %d", decoded.RemainingLength, tt.header.RemainingLength)
			}
		})
	}
}
