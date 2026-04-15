package codec

import "fmt"

// MQTT 3.1.1 packet types (4-bit values in the upper nibble of byte 1).
const (
	CONNECT     byte = 1
	CONNACK     byte = 2
	PUBLISH     byte = 3
	PUBACK      byte = 4
	PUBREC      byte = 5
	PUBREL      byte = 6
	PUBCOMP     byte = 7
	SUBSCRIBE   byte = 8
	SUBACK      byte = 9
	UNSUBSCRIBE byte = 10
	UNSUBACK    byte = 11
	PINGREQ     byte = 12
	PINGRESP    byte = 13
	DISCONNECT  byte = 14
)

// PacketTypeName returns the human-readable name of a packet type.
func PacketTypeName(t byte) string {
	names := map[byte]string{
		CONNECT: "CONNECT", CONNACK: "CONNACK",
		PUBLISH: "PUBLISH", PUBACK: "PUBACK",
		PUBREC: "PUBREC", PUBREL: "PUBREL", PUBCOMP: "PUBCOMP",
		SUBSCRIBE: "SUBSCRIBE", SUBACK: "SUBACK",
		UNSUBSCRIBE: "UNSUBSCRIBE", UNSUBACK: "UNSUBACK",
		PINGREQ: "PINGREQ", PINGRESP: "PINGRESP",
		DISCONNECT: "DISCONNECT",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", t)
}

// MaxRemainingLength is the maximum value of the remaining length field
// as defined in MQTT 3.1.1 spec: 256 MB.
const MaxRemainingLength = 268435455

// ErrMalformedPacket indicates a packet that violates the MQTT spec.
type ErrMalformedPacket struct {
	Reason string
}

func (e *ErrMalformedPacket) Error() string {
	return "malformed packet: " + e.Reason
}
