package cluster

import (
	"encoding/json"
	"fmt"
)

// NodeInfo identifies a node in the cluster.
type NodeInfo struct {
	ID         string `json:"id"`
	Host       string `json:"host"`
	GossipPort int    `json:"gossipPort"`
	MQTTPort   int    `json:"mqttPort"`
	Region     string `json:"region,omitempty"`
}

// GossipAddr returns the address for memberlist communication.
func (n NodeInfo) GossipAddr() string {
	return fmt.Sprintf("%s:%d", n.Host, n.GossipPort)
}

// MQTTAddr returns the address for MQTT client connections.
func (n NodeInfo) MQTTAddr() string {
	return fmt.Sprintf("%s:%d", n.Host, n.MQTTPort)
}

// Marshal serializes NodeInfo to JSON for memberlist metadata.
func (n NodeInfo) Marshal() ([]byte, error) {
	return json.Marshal(n)
}

// UnmarshalNodeInfo deserializes NodeInfo from JSON.
func UnmarshalNodeInfo(data []byte) (NodeInfo, error) {
	var n NodeInfo
	err := json.Unmarshal(data, &n)
	return n, err
}
