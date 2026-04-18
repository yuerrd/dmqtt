package codec

import "encoding/binary"

// Subscription represents a single topic filter + requested QoS in a SUBSCRIBE packet.
type Subscription struct {
	TopicFilter string
	QoS         byte
}

// SubscribePacket represents a decoded MQTT SUBSCRIBE packet.
type SubscribePacket struct {
	PacketID      uint16
	Subscriptions []Subscription
	Properties    *Properties
}

// DecodeSubscribePacket decodes the variable header and payload of a SUBSCRIBE packet.
func DecodeSubscribePacket(data []byte, protocolVersion byte) (*SubscribePacket, error) {
	if len(data) < 5 {
		return nil, &ErrMalformedPacket{Reason: "SUBSCRIBE packet too short"}
	}

	pkt := &SubscribePacket{}
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

		if offset >= len(data) {
			return nil, &ErrMalformedPacket{Reason: "missing QoS byte in SUBSCRIBE"}
		}
		qos := data[offset]
		offset++

		pkt.Subscriptions = append(pkt.Subscriptions, Subscription{
			TopicFilter: topicFilter,
			QoS:         qos,
		})
	}

	if len(pkt.Subscriptions) == 0 {
		return nil, &ErrMalformedPacket{Reason: "SUBSCRIBE must contain at least one topic filter"}
	}

	return pkt, nil
}

// SubackPacket represents an MQTT SUBACK packet.
type SubackPacket struct {
	PacketID        uint16
	ReturnCodes     []byte
	Properties      *Properties
	ProtocolVersion byte
}

// Encode serializes the SUBACK packet to bytes (including fixed header).
func (p *SubackPacket) Encode() []byte {
	if p.ProtocolVersion == 5 {
		propsData := p.Properties.Encode()
		remaining := 2 + len(propsData) + len(p.ReturnCodes)
		fh := FixedHeader{
			PacketType:      SUBACK,
			RemainingLength: remaining,
		}
		buf := fh.Encode()
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
		buf = append(buf, propsData...)
		buf = append(buf, p.ReturnCodes...)
		return buf
	}

	remaining := 2 + len(p.ReturnCodes)
	fh := FixedHeader{
		PacketType:      SUBACK,
		RemainingLength: remaining,
	}

	buf := fh.Encode()
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	buf = append(buf, p.ReturnCodes...)

	return buf
}
