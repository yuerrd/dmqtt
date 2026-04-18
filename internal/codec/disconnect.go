package codec

// DisconnectPacket represents an MQTT DISCONNECT packet.
type DisconnectPacket struct {
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

// Encode serializes the DISCONNECT packet to bytes (including fixed header).
func (p *DisconnectPacket) Encode() []byte {
	if p.ProtocolVersion == 5 {
		propsData := p.Properties.Encode()
		remaining := 1 + len(propsData)
		fh := FixedHeader{PacketType: DISCONNECT, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}
	fh := FixedHeader{PacketType: DISCONNECT, RemainingLength: 0}
	return fh.Encode()
}

// DecodeDisconnectPacket decodes the variable header of a v5 DISCONNECT packet.
func DecodeDisconnectPacket(data []byte) (*DisconnectPacket, error) {
	pkt := &DisconnectPacket{}
	if len(data) == 0 {
		return pkt, nil
	}
	pkt.ReasonCode = data[0]
	if len(data) > 1 {
		propLen, n, err := decodeVarInt(data, 1)
		if err != nil {
			return nil, err
		}
		props, err := DecodeProperties(data[1+n:], propLen)
		if err != nil {
			return nil, err
		}
		pkt.Properties = props
	}
	return pkt, nil
}
