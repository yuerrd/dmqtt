package codec

import "encoding/binary"

// PublishPacket represents a decoded MQTT PUBLISH packet.
type PublishPacket struct {
	Topic           string
	PacketID        uint16
	QoS             byte
	Dup             bool
	Retain          bool
	Payload         []byte
	Properties      *Properties
	ProtocolVersion byte
}

// DecodePublishPacket decodes the variable header and payload of a PUBLISH packet.
// qos is taken from the fixed header. protocolVersion distinguishes v3.1.1 (4) from v5.0 (5).
func DecodePublishPacket(data []byte, qos byte, protocolVersion byte) (*PublishPacket, error) {
	offset := 0
	pkt := &PublishPacket{QoS: qos, ProtocolVersion: protocolVersion}

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

	if protocolVersion == 5 {
		propLen, pn, err := decodeVarInt(data, offset)
		if err != nil {
			return nil, err
		}
		offset += pn
		props, err := DecodeProperties(data[offset:], propLen)
		if err != nil {
			return nil, err
		}
		pkt.Properties = props
		offset += propLen
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

	var propsData []byte
	if p.ProtocolVersion == 5 {
		propsData = p.Properties.Encode()
		remaining += len(propsData)
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

	if p.ProtocolVersion == 5 {
		buf = append(buf, propsData...)
	}

	buf = append(buf, p.Payload...)

	return buf
}

// PubackPacket represents an MQTT PUBACK packet.
type PubackPacket struct {
	PacketID        uint16
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

// Encode serializes the PUBACK packet to bytes (including fixed header).
func (p *PubackPacket) Encode() []byte {
	if p.ProtocolVersion == 5 && (p.ReasonCode != 0x00 || p.Properties != nil) {
		propsData := p.Properties.Encode()
		remaining := 2 + 1 + len(propsData)
		fh := FixedHeader{PacketType: PUBACK, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}
	fh := FixedHeader{
		PacketType:      PUBACK,
		RemainingLength: 2,
	}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

// DecodePubackPacket decodes the variable header of a PUBACK packet.
func DecodePubackPacket(data []byte, protocolVersion byte) (*PubackPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBACK packet too short"}
	}
	pkt := &PubackPacket{
		PacketID:        binary.BigEndian.Uint16(data[:2]),
		ProtocolVersion: protocolVersion,
	}
	offset := 2
	if protocolVersion == 5 && offset < len(data) {
		pkt.ReasonCode = data[offset]
		offset++
		if offset < len(data) {
			propLen, pn, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, err
			}
			offset += pn
			props, err := DecodeProperties(data[offset:], propLen)
			if err != nil {
				return nil, err
			}
			pkt.Properties = props
		}
	}
	return pkt, nil
}

// PubrecPacket represents an MQTT PUBREC packet (QoS 2, step 2).
type PubrecPacket struct {
	PacketID        uint16
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

func (p *PubrecPacket) Encode() []byte {
	if p.ProtocolVersion == 5 && (p.ReasonCode != 0x00 || p.Properties != nil) {
		propsData := p.Properties.Encode()
		remaining := 2 + 1 + len(propsData)
		fh := FixedHeader{PacketType: PUBREC, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}
	fh := FixedHeader{PacketType: PUBREC, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubrecPacket(data []byte, protocolVersion byte) (*PubrecPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBREC packet too short"}
	}
	pkt := &PubrecPacket{
		PacketID:        binary.BigEndian.Uint16(data[:2]),
		ProtocolVersion: protocolVersion,
	}
	offset := 2
	if protocolVersion == 5 && offset < len(data) {
		pkt.ReasonCode = data[offset]
		offset++
		if offset < len(data) {
			propLen, pn, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, err
			}
			offset += pn
			props, err := DecodeProperties(data[offset:], propLen)
			if err != nil {
				return nil, err
			}
			pkt.Properties = props
		}
	}
	return pkt, nil
}

// PubrelPacket represents an MQTT PUBREL packet (QoS 2, step 3).
type PubrelPacket struct {
	PacketID        uint16
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

func (p *PubrelPacket) Encode() []byte {
	if p.ProtocolVersion == 5 && (p.ReasonCode != 0x00 || p.Properties != nil) {
		propsData := p.Properties.Encode()
		remaining := 2 + 1 + len(propsData)
		fh := FixedHeader{PacketType: PUBREL, QoS: 1, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}
	// PUBREL requires fixed header flags 0x02 per MQTT 3.1.1 spec
	fh := FixedHeader{PacketType: PUBREL, QoS: 1, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubrelPacket(data []byte, protocolVersion byte) (*PubrelPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBREL packet too short"}
	}
	pkt := &PubrelPacket{
		PacketID:        binary.BigEndian.Uint16(data[:2]),
		ProtocolVersion: protocolVersion,
	}
	offset := 2
	if protocolVersion == 5 && offset < len(data) {
		pkt.ReasonCode = data[offset]
		offset++
		if offset < len(data) {
			propLen, pn, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, err
			}
			offset += pn
			props, err := DecodeProperties(data[offset:], propLen)
			if err != nil {
				return nil, err
			}
			pkt.Properties = props
		}
	}
	return pkt, nil
}

// PubcompPacket represents an MQTT PUBCOMP packet (QoS 2, step 4).
type PubcompPacket struct {
	PacketID        uint16
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

func (p *PubcompPacket) Encode() []byte {
	if p.ProtocolVersion == 5 && (p.ReasonCode != 0x00 || p.Properties != nil) {
		propsData := p.Properties.Encode()
		remaining := 2 + 1 + len(propsData)
		fh := FixedHeader{PacketType: PUBCOMP, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}
	fh := FixedHeader{PacketType: PUBCOMP, RemainingLength: 2}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}

func DecodePubcompPacket(data []byte, protocolVersion byte) (*PubcompPacket, error) {
	if len(data) < 2 {
		return nil, &ErrMalformedPacket{Reason: "PUBCOMP packet too short"}
	}
	pkt := &PubcompPacket{
		PacketID:        binary.BigEndian.Uint16(data[:2]),
		ProtocolVersion: protocolVersion,
	}
	offset := 2
	if protocolVersion == 5 && offset < len(data) {
		pkt.ReasonCode = data[offset]
		offset++
		if offset < len(data) {
			propLen, pn, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, err
			}
			offset += pn
			props, err := DecodeProperties(data[offset:], propLen)
			if err != nil {
				return nil, err
			}
			pkt.Properties = props
		}
	}
	return pkt, nil
}
