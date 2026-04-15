package codec

// DISCONNECT has no variable header or payload in MQTT 3.1.1.
// Detection is handled by checking FixedHeader.PacketType == DISCONNECT.
