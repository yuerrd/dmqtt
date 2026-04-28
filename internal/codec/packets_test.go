package codec

import "testing"

func TestPacketTypeName(t *testing.T) {
	tests := []struct {
		packetType byte
		want       string
	}{
		{CONNECT, "CONNECT"},
		{CONNACK, "CONNACK"},
		{PUBLISH, "PUBLISH"},
		{PUBACK, "PUBACK"},
		{PUBREC, "PUBREC"},
		{PUBREL, "PUBREL"},
		{PUBCOMP, "PUBCOMP"},
		{SUBSCRIBE, "SUBSCRIBE"},
		{SUBACK, "SUBACK"},
		{UNSUBSCRIBE, "UNSUBSCRIBE"},
		{UNSUBACK, "UNSUBACK"},
		{PINGREQ, "PINGREQ"},
		{PINGRESP, "PINGRESP"},
		{DISCONNECT, "DISCONNECT"},
		{0, "UNKNOWN(0)"},
		{15, "UNKNOWN(15)"},
		{255, "UNKNOWN(255)"},
	}
	for _, tt := range tests {
		got := PacketTypeName(tt.packetType)
		if got != tt.want {
			t.Errorf("PacketTypeName(%d) = %q, want %q", tt.packetType, got, tt.want)
		}
	}
}

func TestErrMalformedPacket_Error(t *testing.T) {
	err := &ErrMalformedPacket{Reason: "missing field"}
	got := err.Error()
	want := "malformed packet: missing field"
	if got != want {
		t.Errorf("ErrMalformedPacket.Error() = %q, want %q", got, want)
	}
}
