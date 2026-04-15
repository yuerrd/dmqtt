package codec

import (
	"bytes"
	"testing"
)

func TestPingreqEncode(t *testing.T) {
	data := EncodePingresp()
	expected := []byte{0xD0, 0x00}
	if !bytes.Equal(data, expected) {
		t.Errorf("EncodePingresp() = %v, want %v", data, expected)
	}
}
