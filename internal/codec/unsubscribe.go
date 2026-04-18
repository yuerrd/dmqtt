package codec

import "encoding/binary"

// UnsubscribePacket represents a decoded MQTT UNSUBSCRIBE packet.
type UnsubscribePacket struct {
	PacketID     uint16
	TopicFilters []string
	Properties   *Properties
}

// DecodeUnsubscribePacket decodes the variable header and payload of an UNSUBSCRIBE packet.
func DecodeUnsubscribePacket(data []byte, protocolVersion byte) (*UnsubscribePacket, error) {
	if len(data) < 4 {
		return nil, &ErrMalformedPacket{Reason: "UNSUBSCRIBE packet too short"}
	}

	pkt := &UnsubscribePacket{}
	pkt.PacketID = binary.BigEndian.Uint16(data[:2])
	offset := 2

	if protocolVersion == 5 {
		propLen, n, err := decodeVarInt(data, offset)
		if err != nil {
			return nil, err
		}
		offset += n
		props, err := DecodeProperties(data[offset:], propLen)
		if err != nil {
			return nil, err
		}
		pkt.Properties = props
		offset += propLen
	}

	for offset < len(data) {
		topicFilter, n, err := readUTF8String(data, offset)
		if err != nil {
			return nil, err
		}
		offset += n
		pkt.TopicFilters = append(pkt.TopicFilters, topicFilter)
	}

	if len(pkt.TopicFilters) == 0 {
		return nil, &ErrMalformedPacket{Reason: "UNSUBSCRIBE must contain at least one topic filter"}
	}

	return pkt, nil
}

// UnsubackPacket represents an MQTT UNSUBACK packet.
type UnsubackPacket struct {
	PacketID        uint16
	ReasonCodes     []byte
	Properties      *Properties
	ProtocolVersion byte
}

// Encode serializes the UNSUBACK packet to bytes (including fixed header).
func (p *UnsubackPacket) Encode() []byte {
	if p.ProtocolVersion == 5 {
		propsData := p.Properties.Encode()
		remaining := 2 + len(propsData) + len(p.ReasonCodes)
		fh := FixedHeader{
			PacketType:      UNSUBACK,
			RemainingLength: remaining,
		}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, propsData...)
		buf = append(buf, p.ReasonCodes...)
		return buf
	}

	fh := FixedHeader{
		PacketType:      UNSUBACK,
		RemainingLength: 2,
	}
	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	return buf
}
