package broker

// WillMessage holds the last will and testament for a client.
type WillMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}
