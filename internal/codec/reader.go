package codec

import "io"

// ReadPacket reads one complete MQTT packet from r.
// Returns the fixed header and the remaining bytes (variable header + payload).
func ReadPacket(r io.Reader) (*FixedHeader, []byte, error) {
	fh, err := DecodeFixedHeader(r)
	if err != nil {
		return nil, nil, err
	}

	var data []byte
	if fh.RemainingLength > 0 {
		data = make([]byte, fh.RemainingLength)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, nil, err
		}
	}

	return fh, data, nil
}
