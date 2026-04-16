package codec

import "encoding/binary"

// PublishPacket represents a decoded MQTT PUBLISH packet.
type PublishPacket struct {
	Topic    string
	PacketID uint16
	QoS      byte
	Dup      bool
	Retain   bool
	Payload  []byte
}

// DecodePublishPacket decodes the variable header and payload of a PUBLISH packet.
// qos is taken from the fixed header.
func DecodePublishPacket(data []byte, qos byte) (*PublishPacket, error) {
	offset := 0
	pkt := &PublishPacket{QoS: qos}

	topic, n, err := readUTF8String(data, offset)
	if err != nil {
		return nil, err
	}
	pkt.Topic = topic
	offset += n

	if qos > 0 {
		if offset+2 > len(data) {
			return nil, &ErrMalformedPacket{Reason: "missing packet ID in PUBLISH"}
		}
		pkt.PacketID = binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2
	}

	if offset < len(data) {
		pkt.Payload = make([]byte, len(data)-offset)
		copy(pkt.Payload, data[offset:])
	}

	return pkt, nil
}

// Encode serializes the PUBLISH packet to bytes (including fixed header).
func (p *PublishPacket) Encode() []byte {
	topicLen := 2 + len(p.Topic)
	remaining := topicLen + len(p.Payload)
	if p.QoS > 0 {
		remaining += 2
	}

	fh := FixedHeader{
		PacketType:      PUBLISH,
		Dup:             p.Dup,
		QoS:             p.QoS,
		Retain:          p.Retain,
		RemainingLength: remaining,
	}

	buf := fh.Encode()
	buf = append(buf, byte(len(p.Topic)>>8), byte(len(p.Topic)))
	buf = append(buf, []byte(p.Topic)...)

	if p.QoS > 0 {
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	}

	buf = append(buf, p.Payload...)

	return buf
}

// PubackPacket represents an MQTT PUBACK packet.
type PubackPacket struct {
	PacketID uint16
}

// Encode serializes the PUBACK packet to bytes (including fixed header).
func (p *PubackPacket) Encode() []byte {
	fh := FixedHeader{
		PacketType:      PUBACK,
		RemainingLength: 2,
	}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

// DecodePubackPacket decodes the variable header of a PUBACK packet.
func DecodePubackPacket(data []byte) (*PubackPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBACK packet too short"}
	}
	return &PubackPacket{
		PacketID: binary.BigEndian.Uint16(data[:2]),
	}, nil
}

// PubrecPacket represents an MQTT PUBREC packet (QoS 2, step 2).
type PubrecPacket struct {
	PacketID uint16
}

func (p *PubrecPacket) Encode() []byte {
	fh := FixedHeader{PacketType: PUBREC, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubrecPacket(data []byte) (*PubrecPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBREC packet too short"}
	}
	return &PubrecPacket{PacketID: binary.BigEndian.Uint16(data[:2])}, nil
}

// PubrelPacket represents an MQTT PUBREL packet (QoS 2, step 3).
type PubrelPacket struct {
	PacketID uint16
}

func (p *PubrelPacket) Encode() []byte {
	// PUBREL requires fixed header flags 0x02 per MQTT 3.1.1 spec
	fh := FixedHeader{PacketType: PUBREL, QoS: 1, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubrelPacket(data []byte) (*PubrelPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBREL packet too short"}
	}
	return &PubrelPacket{PacketID: binary.BigEndian.Uint16(data[:2])}, nil
}

// PubcompPacket represents an MQTT PUBCOMP packet (QoS 2, step 4).
type PubcompPacket struct {
	PacketID uint16
}

func (p *PubcompPacket) Encode() []byte {
	fh := FixedHeader{PacketType: PUBCOMP, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubcompPacket(data []byte) (*PubcompPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBCOMP packet too short"}
	}
	return &PubcompPacket{PacketID: binary.BigEndian.Uint16(data[:2])}, nil
}
