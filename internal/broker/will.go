package broker

// WillMessage holds the last will and testament for a client.
type WillMessage struct {
	Topic       string `json:"topic"`
	Payload     []byte `json:"payload"`
	QoS         byte   `json:"qos"`
	Retain      bool   `json:"retain"`
	ClientID    string `json:"clientID,omitempty"`
	PersistedAt int64  `json:"persistedAt,omitempty"`
}
