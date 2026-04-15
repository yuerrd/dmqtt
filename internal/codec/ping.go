package codec

// EncodePingresp creates a PINGRESP packet (fixed header only).
func EncodePingresp() []byte {
	return []byte{byte(PINGRESP) << 4, 0x00}
}
