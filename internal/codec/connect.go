package codec

import (
	"encoding/binary"
	"fmt"
)

// CONNACK return codes (MQTT 3.1.1 §3.2.2.3).
const (
	ConnackAccepted                byte = 0x00
	ConnackUnacceptableProtocol    byte = 0x01
	ConnackIdentifierRejected      byte = 0x02
	ConnackServerUnavailable       byte = 0x03
	ConnackBadUsernameOrPassword   byte = 0x04
	ConnackNotAuthorized           byte = 0x05
)

// ConnectPacket represents a decoded MQTT CONNECT packet.
type ConnectPacket struct {
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	KeepAlive     uint16
	ClientID      string

	// Will fields
	WillFlag    bool
	WillQoS     byte
	WillRetain  bool
	WillTopic   string
	WillPayload []byte

	// Auth fields
	UsernameFlag bool
	PasswordFlag bool
	Username     string
	Password     []byte

	Properties     *Properties // v5.0 CONNECT properties
	WillProperties *Properties // v5.0 Will properties
}

// DecodeConnectPacket decodes the variable header and payload of a CONNECT packet.
// data should contain everything after the fixed header.
func DecodeConnectPacket(data []byte) (*ConnectPacket, error) {
	if len(data) < 10 {
		return nil, &ErrMalformedPacket{Reason: "CONNECT packet too short"}
	}

	offset := 0
	pkt := &ConnectPacket{}

	// Protocol Name (UTF-8)
	name, n, err := readUTF8String(data, offset)
	if err != nil {
		return nil, fmt.Errorf("reading protocol name: %w", err)
	}
	pkt.ProtocolName = name
	offset += n

	// Protocol Level
	if offset >= len(data) {
		return nil, &ErrMalformedPacket{Reason: "missing protocol level"}
	}
	pkt.ProtocolLevel = data[offset]
	offset++

	if pkt.ProtocolLevel != 4 && pkt.ProtocolLevel != 5 {
		return nil, &ErrMalformedPacket{Reason: fmt.Sprintf("unsupported protocol level %d", pkt.ProtocolLevel)}
	}

	// Connect Flags
	if offset >= len(data) {
		return nil, &ErrMalformedPacket{Reason: "missing connect flags"}
	}
	flags := data[offset]
	offset++

	pkt.CleanSession = (flags & 0x02) != 0
	pkt.WillFlag = (flags & 0x04) != 0
	pkt.WillQoS = (flags >> 3) & 0x03
	pkt.WillRetain = (flags & 0x20) != 0
	pkt.PasswordFlag = (flags & 0x40) != 0
	pkt.UsernameFlag = (flags & 0x80) != 0

	// Keep Alive
	if offset+2 > len(data) {
		return nil, &ErrMalformedPacket{Reason: "missing keep alive"}
	}
	pkt.KeepAlive = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// v5: decode Connect Properties
	if pkt.ProtocolLevel == 5 {
		propLen, n, err := decodeVarInt(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reading connect properties length: %w", err)
		}
		offset += n
		props, err := DecodeProperties(data[offset:], propLen)
		if err != nil {
			return nil, fmt.Errorf("decoding connect properties: %w", err)
		}
		pkt.Properties = props
		offset += propLen
	}

	// Payload: Client ID
	clientID, n, err := readUTF8String(data, offset)
	if err != nil {
		return nil, fmt.Errorf("reading client ID: %w", err)
	}
	pkt.ClientID = clientID
	offset += n

	// Will Topic and Will Message
	if pkt.WillFlag {
		// v5: decode Will Properties before will topic/payload
		if pkt.ProtocolLevel == 5 {
			propLen, n, err := decodeVarInt(data, offset)
			if err != nil {
				return nil, fmt.Errorf("reading will properties length: %w", err)
			}
			offset += n
			willProps, err := DecodeProperties(data[offset:], propLen)
			if err != nil {
				return nil, fmt.Errorf("decoding will properties: %w", err)
			}
			pkt.WillProperties = willProps
			offset += propLen
		}

		willTopic, n, err := readUTF8String(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reading will topic: %w", err)
		}
		pkt.WillTopic = willTopic
		offset += n

		willPayload, n, err := readBinaryData(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reading will payload: %w", err)
		}
		pkt.WillPayload = willPayload
		offset += n
	}

	// Username
	if pkt.UsernameFlag {
		username, n, err := readUTF8String(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reading username: %w", err)
		}
		pkt.Username = username
		offset += n
	}

	// Password
	if pkt.PasswordFlag {
		password, n, err := readBinaryData(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reading password: %w", err)
		}
		pkt.Password = password
		offset += n
	}

	return pkt, nil
}

// ConnackPacket represents an MQTT CONNACK packet.
type ConnackPacket struct {
	SessionPresent  bool
	ReturnCode      byte
	ReasonCode      byte
	Properties      *Properties
	ProtocolVersion byte
}

// Encode serializes the CONNACK packet to bytes (including fixed header).
func (p *ConnackPacket) Encode() []byte {
	if p.ProtocolVersion == 5 {
		var sp byte
		if p.SessionPresent {
			sp = 0x01
		}
		propsData := p.Properties.Encode()
		remaining := 2 + len(propsData)
		fh := FixedHeader{PacketType: CONNACK, RemainingLength: remaining}
		buf := fh.Encode()
		buf = append(buf, sp, p.ReasonCode)
		buf = append(buf, propsData...)
		return buf
	}

	// v3.1.1
	fh := FixedHeader{
		PacketType:      CONNACK,
		RemainingLength: 2,
	}

	buf := fh.Encode()

	var sp byte
	if p.SessionPresent {
		sp = 0x01
	}
	buf = append(buf, sp, p.ReturnCode)

	return buf
}

// readUTF8String reads a 2-byte length-prefixed UTF-8 string from data at offset.
// Returns the string, total bytes consumed, and any error.
func readUTF8String(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", 0, &ErrMalformedPacket{Reason: "insufficient data for UTF-8 string length"}
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	if offset+2+length > len(data) {
		return "", 0, &ErrMalformedPacket{Reason: "insufficient data for UTF-8 string"}
	}
	return string(data[offset+2 : offset+2+length]), 2 + length, nil
}

// readBinaryData reads a 2-byte length-prefixed binary data from data at offset.
func readBinaryData(data []byte, offset int) ([]byte, int, error) {
	if offset+2 > len(data) {
		return nil, 0, &ErrMalformedPacket{Reason: "insufficient data for binary data length"}
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	if offset+2+length > len(data) {
		return nil, 0, &ErrMalformedPacket{Reason: "insufficient data for binary data"}
	}
	result := make([]byte, length)
	copy(result, data[offset+2:offset+2+length])
	return result, 2 + length, nil
}
