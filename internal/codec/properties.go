package codec

import "encoding/binary"

// MQTT 5.0 Property Identifiers (§2.2.2.2)
const (
	PropPayloadFormatIndicator   byte = 0x01
	PropMessageExpiryInterval    byte = 0x02
	PropContentType              byte = 0x03
	PropResponseTopic            byte = 0x08
	PropCorrelationData          byte = 0x09
	PropSubscriptionIdentifier   byte = 0x0B
	PropSessionExpiryInterval    byte = 0x11
	PropAssignedClientIdentifier byte = 0x12
	PropServerKeepAlive          byte = 0x13
	PropAuthenticationMethod     byte = 0x15
	PropAuthenticationData       byte = 0x16
	PropRequestProblemInfo       byte = 0x17
	PropWillDelayInterval        byte = 0x18
	PropRequestResponseInfo      byte = 0x19
	PropResponseInformation      byte = 0x1A
	PropServerReference          byte = 0x1C
	PropReasonString             byte = 0x1F
	PropReceiveMaximum           byte = 0x21
	PropTopicAliasMaximum        byte = 0x22
	PropTopicAlias               byte = 0x23
	PropMaximumQoS               byte = 0x24
	PropRetainAvailable          byte = 0x25
	PropUserProperty             byte = 0x26
	PropMaximumPacketSize        byte = 0x27
	PropWildcardSubAvailable     byte = 0x28
	PropSubIdentifierAvailable   byte = 0x29
	PropSharedSubAvailable       byte = 0x2A
)

// UserProperty is a key-value string pair used in MQTT 5.0 packets.
type UserProperty struct {
	Key   string
	Value string
}

// Properties holds all MQTT 5.0 properties. Pointer fields indicate "not set" when nil.
type Properties struct {
	SessionExpiryInterval    *uint32
	ReceiveMaximum           *uint16
	MaximumQoS               *byte
	RetainAvailable          *byte
	MaximumPacketSize        *uint32
	AssignedClientIdentifier *string
	TopicAliasMaximum        *uint16
	ReasonString             *string
	WildcardSubAvailable     *byte
	SubIdentifierAvailable   *byte
	SharedSubAvailable       *byte
	ServerKeepAlive          *uint16
	ServerReference          *string
	ResponseInformation      *string

	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	TopicAlias             *uint16
	ResponseTopic          *string
	CorrelationData        []byte
	ContentType            *string
	SubscriptionIdentifier *uint32

	WillDelayInterval *uint32

	AuthenticationMethod *string
	AuthenticationData   []byte

	RequestProblemInfo  *byte
	RequestResponseInfo *byte
	UserProperties      []UserProperty
}

// Encode serializes properties to bytes with a variable-length prefix.
func (p *Properties) Encode() []byte {
	if p == nil {
		return []byte{0x00}
	}

	var body []byte

	body = appendUint32Prop(body, PropSessionExpiryInterval, p.SessionExpiryInterval)
	body = appendUint16Prop(body, PropReceiveMaximum, p.ReceiveMaximum)
	body = appendByteProp(body, PropMaximumQoS, p.MaximumQoS)
	body = appendByteProp(body, PropRetainAvailable, p.RetainAvailable)
	body = appendUint32Prop(body, PropMaximumPacketSize, p.MaximumPacketSize)
	body = appendStringProp(body, PropAssignedClientIdentifier, p.AssignedClientIdentifier)
	body = appendUint16Prop(body, PropTopicAliasMaximum, p.TopicAliasMaximum)
	body = appendStringProp(body, PropReasonString, p.ReasonString)
	body = appendByteProp(body, PropWildcardSubAvailable, p.WildcardSubAvailable)
	body = appendByteProp(body, PropSubIdentifierAvailable, p.SubIdentifierAvailable)
	body = appendByteProp(body, PropSharedSubAvailable, p.SharedSubAvailable)
	body = appendUint16Prop(body, PropServerKeepAlive, p.ServerKeepAlive)
	body = appendStringProp(body, PropServerReference, p.ServerReference)
	body = appendStringProp(body, PropResponseInformation, p.ResponseInformation)

	body = appendByteProp(body, PropPayloadFormatIndicator, p.PayloadFormatIndicator)
	body = appendUint32Prop(body, PropMessageExpiryInterval, p.MessageExpiryInterval)
	body = appendUint16Prop(body, PropTopicAlias, p.TopicAlias)
	body = appendStringProp(body, PropResponseTopic, p.ResponseTopic)
	body = appendBinaryProp(body, PropCorrelationData, p.CorrelationData)
	body = appendStringProp(body, PropContentType, p.ContentType)
	if p.SubscriptionIdentifier != nil {
		body = append(body, PropSubscriptionIdentifier)
		body = append(body, encodeVarInt(int(*p.SubscriptionIdentifier))...)
	}

	body = appendUint32Prop(body, PropWillDelayInterval, p.WillDelayInterval)

	body = appendStringProp(body, PropAuthenticationMethod, p.AuthenticationMethod)
	body = appendBinaryProp(body, PropAuthenticationData, p.AuthenticationData)

	body = appendByteProp(body, PropRequestProblemInfo, p.RequestProblemInfo)
	body = appendByteProp(body, PropRequestResponseInfo, p.RequestResponseInfo)

	for _, up := range p.UserProperties {
		body = append(body, PropUserProperty)
		body = appendUTF8(body, up.Key)
		body = appendUTF8(body, up.Value)
	}

	result := encodeVarInt(len(body))
	result = append(result, body...)
	return result
}

// DecodeProperties decodes properties from data of the given propertyLength.
func DecodeProperties(data []byte, propertyLength int) (*Properties, error) {
	if propertyLength == 0 {
		return &Properties{}, nil
	}
	if propertyLength > len(data) {
		return nil, &ErrMalformedPacket{Reason: "property length exceeds available data"}
	}

	p := &Properties{}
	offset := 0
	end := propertyLength

	for offset < end {
		if offset >= len(data) {
			return nil, &ErrMalformedPacket{Reason: "truncated property"}
		}
		propID := data[offset]
		offset++

		switch propID {
		case PropSessionExpiryInterval:
			v, n, err := readUint32(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.SessionExpiryInterval = &v
			offset += n
		case PropReceiveMaximum:
			v, n, err := readUint16(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ReceiveMaximum = &v
			offset += n
		case PropMaximumQoS:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.MaximumQoS = &v
			offset += n
		case PropRetainAvailable:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.RetainAvailable = &v
			offset += n
		case PropMaximumPacketSize:
			v, n, err := readUint32(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.MaximumPacketSize = &v
			offset += n
		case PropAssignedClientIdentifier:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.AssignedClientIdentifier = &v
			offset += n
		case PropTopicAliasMaximum:
			v, n, err := readUint16(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.TopicAliasMaximum = &v
			offset += n
		case PropReasonString:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ReasonString = &v
			offset += n
		case PropWildcardSubAvailable:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.WildcardSubAvailable = &v
			offset += n
		case PropSubIdentifierAvailable:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.SubIdentifierAvailable = &v
			offset += n
		case PropSharedSubAvailable:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.SharedSubAvailable = &v
			offset += n
		case PropServerKeepAlive:
			v, n, err := readUint16(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ServerKeepAlive = &v
			offset += n
		case PropServerReference:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ServerReference = &v
			offset += n
		case PropResponseInformation:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ResponseInformation = &v
			offset += n
		case PropPayloadFormatIndicator:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.PayloadFormatIndicator = &v
			offset += n
		case PropMessageExpiryInterval:
			v, n, err := readUint32(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.MessageExpiryInterval = &v
			offset += n
		case PropTopicAlias:
			v, n, err := readUint16(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.TopicAlias = &v
			offset += n
		case PropResponseTopic:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ResponseTopic = &v
			offset += n
		case PropCorrelationData:
			v, n, err := readPropBinaryData(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.CorrelationData = v
			offset += n
		case PropContentType:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.ContentType = &v
			offset += n
		case PropSubscriptionIdentifier:
			v, n, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, err
			}
			val := uint32(v)
			p.SubscriptionIdentifier = &val
			offset += n
		case PropWillDelayInterval:
			v, n, err := readUint32(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.WillDelayInterval = &v
			offset += n
		case PropAuthenticationMethod:
			v, n, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.AuthenticationMethod = &v
			offset += n
		case PropAuthenticationData:
			v, n, err := readPropBinaryData(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.AuthenticationData = v
			offset += n
		case PropRequestProblemInfo:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.RequestProblemInfo = &v
			offset += n
		case PropRequestResponseInfo:
			v, n, err := readByte(data, offset, end)
			if err != nil {
				return nil, err
			}
			p.RequestResponseInfo = &v
			offset += n
		case PropUserProperty:
			key, n1, err := readPropString(data, offset, end)
			if err != nil {
				return nil, err
			}
			value, n2, err := readPropString(data, offset+n1, end)
			if err != nil {
				return nil, err
			}
			p.UserProperties = append(p.UserProperties, UserProperty{Key: key, Value: value})
			offset += n1 + n2
		default:
			return nil, &ErrMalformedPacket{Reason: "unknown property identifier"}
		}
	}

	return p, nil
}

// encodeVarInt encodes a variable-length integer (1-4 bytes).
func encodeVarInt(n int) []byte {
	var buf []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if n == 0 {
			break
		}
	}
	return buf
}

// decodeVarInt decodes a variable-length integer from data at offset.
// Returns the value and number of bytes consumed.
func decodeVarInt(data []byte, offset int) (int, int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		if offset+i >= len(data) {
			return 0, 0, &ErrMalformedPacket{Reason: "truncated variable byte integer"}
		}
		b := data[offset+i]
		value += int(b&0x7F) * multiplier
		if b&0x80 == 0 {
			return value, i + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, &ErrMalformedPacket{Reason: "variable byte integer exceeds 4 bytes"}
}

// --- encoding helpers ---

func appendByteProp(buf []byte, id byte, val *byte) []byte {
	if val == nil {
		return buf
	}
	return append(buf, id, *val)
}

func appendUint16Prop(buf []byte, id byte, val *uint16) []byte {
	if val == nil {
		return buf
	}
	buf = append(buf, id)
	buf = append(buf, byte(*val>>8), byte(*val))
	return buf
}

func appendUint32Prop(buf []byte, id byte, val *uint32) []byte {
	if val == nil {
		return buf
	}
	buf = append(buf, id)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, *val)
	buf = append(buf, b...)
	return buf
}

func appendStringProp(buf []byte, id byte, val *string) []byte {
	if val == nil {
		return buf
	}
	buf = append(buf, id)
	buf = appendUTF8(buf, *val)
	return buf
}

func appendBinaryProp(buf []byte, id byte, val []byte) []byte {
	if val == nil {
		return buf
	}
	buf = append(buf, id)
	buf = append(buf, byte(len(val)>>8), byte(len(val)))
	buf = append(buf, val...)
	return buf
}

func appendUTF8(buf []byte, s string) []byte {
	buf = append(buf, byte(len(s)>>8), byte(len(s)))
	buf = append(buf, []byte(s)...)
	return buf
}

// --- decoding helpers ---

func readByte(data []byte, offset, end int) (byte, int, error) {
	if offset >= end || offset >= len(data) {
		return 0, 0, &ErrMalformedPacket{Reason: "truncated byte property"}
	}
	return data[offset], 1, nil
}

func readUint16(data []byte, offset, end int) (uint16, int, error) {
	if offset+2 > end || offset+2 > len(data) {
		return 0, 0, &ErrMalformedPacket{Reason: "truncated uint16 property"}
	}
	return binary.BigEndian.Uint16(data[offset : offset+2]), 2, nil
}

func readUint32(data []byte, offset, end int) (uint32, int, error) {
	if offset+4 > end || offset+4 > len(data) {
		return 0, 0, &ErrMalformedPacket{Reason: "truncated uint32 property"}
	}
	return binary.BigEndian.Uint32(data[offset : offset+4]), 4, nil
}

func readPropString(data []byte, offset, end int) (string, int, error) {
	if offset+2 > end || offset+2 > len(data) {
		return "", 0, &ErrMalformedPacket{Reason: "truncated string property length"}
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	if offset+2+length > end || offset+2+length > len(data) {
		return "", 0, &ErrMalformedPacket{Reason: "truncated string property"}
	}
	return string(data[offset+2 : offset+2+length]), 2 + length, nil
}

func readPropBinaryData(data []byte, offset, end int) ([]byte, int, error) {
	if offset+2 > end || offset+2 > len(data) {
		return nil, 0, &ErrMalformedPacket{Reason: "truncated binary data length"}
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	if offset+2+length > end || offset+2+length > len(data) {
		return nil, 0, &ErrMalformedPacket{Reason: "truncated binary data"}
	}
	result := make([]byte, length)
	copy(result, data[offset+2:offset+2+length])
	return result, 2 + length, nil
}
