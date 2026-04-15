package codec

import "io"

// FixedHeader represents the MQTT fixed header present in every packet.
type FixedHeader struct {
	PacketType      byte // 4-bit packet type (1-14)
	Dup             bool // Duplicate delivery (PUBLISH only)
	QoS             byte // Quality of Service (0, 1, or 2; PUBLISH only)
	Retain          bool // Retain flag (PUBLISH only)
	RemainingLength int  // Length of variable header + payload
}

// Encode serializes the fixed header to bytes.
func (fh *FixedHeader) Encode() []byte {
	var firstByte byte
	firstByte = fh.PacketType << 4
	if fh.Dup {
		firstByte |= 0x08
	}
	firstByte |= (fh.QoS & 0x03) << 1
	if fh.Retain {
		firstByte |= 0x01
	}

	buf := []byte{firstByte}
	buf = append(buf, EncodeRemainingLength(fh.RemainingLength)...)
	return buf
}

// DecodeFixedHeader reads and decodes a fixed header from r.
func DecodeFixedHeader(r io.Reader) (*FixedHeader, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return nil, err
	}

	fh := &FixedHeader{
		PacketType: (b[0] >> 4) & 0x0F,
		Dup:        (b[0] & 0x08) != 0,
		QoS:        (b[0] >> 1) & 0x03,
		Retain:     (b[0] & 0x01) != 0,
	}

	remaining, err := DecodeRemainingLength(r)
	if err != nil {
		return nil, err
	}
	fh.RemainingLength = remaining

	return fh, nil
}

// EncodeRemainingLength encodes an integer into the MQTT variable-length format.
func EncodeRemainingLength(length int) []byte {
	var encoded []byte
	for {
		encodedByte := byte(length % 128)
		length /= 128
		if length > 0 {
			encodedByte |= 0x80
		}
		encoded = append(encoded, encodedByte)
		if length == 0 {
			break
		}
	}
	return encoded
}

// DecodeRemainingLength decodes the MQTT variable-length remaining length from r.
func DecodeRemainingLength(r io.Reader) (int, error) {
	var multiplier int = 1
	var value int
	var b [1]byte

	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		value += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}

	return 0, &ErrMalformedPacket{Reason: "remaining length exceeds 4 bytes"}
}
