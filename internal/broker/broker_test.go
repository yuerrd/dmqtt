package broker

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/langzp/dmqtt/internal/codec"
)

func mqttConnect(t *testing.T, conn net.Conn, clientID string, cleanSession bool) {
	t.Helper()

	var payload bytes.Buffer
	writeUTF8(&payload, "MQTT")
	payload.WriteByte(0x04)
	flags := byte(0x00)
	if cleanSession {
		flags |= 0x02
	}
	payload.WriteByte(flags)
	payload.Write([]byte{0x00, 0x3C})
	writeUTF8(&payload, clientID)

	fh := codec.FixedHeader{
		PacketType:      codec.CONNECT,
		RemainingLength: payload.Len(),
	}
	conn.Write(fh.Encode())
	conn.Write(payload.Bytes())

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respFH, data, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading CONNACK: %v", err)
	}
	if respFH.PacketType != codec.CONNACK {
		t.Fatalf("expected CONNACK, got %s", codec.PacketTypeName(respFH.PacketType))
	}
	if len(data) < 2 || data[1] != codec.ConnackAccepted {
		t.Fatalf("CONNACK return code: %v", data)
	}
	conn.SetReadDeadline(time.Time{})
}

func mqttSubscribe(t *testing.T, conn net.Conn, packetID uint16, filter string, qos byte) {
	t.Helper()

	var payload bytes.Buffer
	payload.Write([]byte{byte(packetID >> 8), byte(packetID)})
	writeUTF8(&payload, filter)
	payload.WriteByte(qos)

	fh := codec.FixedHeader{
		PacketType:      codec.SUBSCRIBE,
		QoS:             1,
		RemainingLength: payload.Len(),
	}
	conn.Write(fh.Encode())
	conn.Write(payload.Bytes())

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respFH, _, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading SUBACK: %v", err)
	}
	if respFH.PacketType != codec.SUBACK {
		t.Fatalf("expected SUBACK, got %s", codec.PacketTypeName(respFH.PacketType))
	}
	conn.SetReadDeadline(time.Time{})
}

func mqttPublishQoS0(t *testing.T, conn net.Conn, topic string, payload []byte) {
	t.Helper()

	pkt := &codec.PublishPacket{
		Topic:   topic,
		QoS:     0,
		Payload: payload,
	}
	conn.Write(pkt.Encode())
}

func mqttReadPublish(t *testing.T, conn net.Conn, timeout time.Duration) *codec.PublishPacket {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(timeout))
	fh, data, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading PUBLISH: %v", err)
	}
	if fh.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH, got %s", codec.PacketTypeName(fh.PacketType))
	}
	pkt, err := codec.DecodePublishPacket(data, fh.QoS)
	if err != nil {
		t.Fatalf("decoding PUBLISH: %v", err)
	}
	conn.SetReadDeadline(time.Time{})
	return pkt
}

func writeUTF8(buf *bytes.Buffer, s string) {
	buf.WriteByte(byte(len(s) >> 8))
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
}

func TestBroker_ConnectDisconnect(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	conn := dial(t, b.Addr())
	defer conn.Close()

	mqttConnect(t, conn, "test-client", true)
}

func TestBroker_PubSubQoS0(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "subscriber", true)
	mqttSubscribe(t, sub, 1, "test/topic", 0)

	time.Sleep(50 * time.Millisecond)

	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "publisher", true)

	mqttPublishQoS0(t, pub, "test/topic", []byte("hello"))

	pkt := mqttReadPublish(t, sub, 2*time.Second)
	if pkt.Topic != "test/topic" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "test/topic")
	}
	if string(pkt.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", pkt.Payload, "hello")
	}
}

func TestBroker_WildcardSubscription(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "wildcard-sub", true)
	mqttSubscribe(t, sub, 1, "sensor/+/temp", 0)

	time.Sleep(50 * time.Millisecond)

	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "wildcard-pub", true)

	mqttPublishQoS0(t, pub, "sensor/room1/temp", []byte("25.5"))

	pkt := mqttReadPublish(t, sub, 2*time.Second)
	if pkt.Topic != "sensor/room1/temp" {
		t.Errorf("Topic = %q, want %q", pkt.Topic, "sensor/room1/temp")
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	var subs [3]net.Conn
	for i := range subs {
		subs[i] = dial(t, b.Addr())
		defer subs[i].Close()
		mqttConnect(t, subs[i], "multi-sub-"+string(rune('A'+i)), true)
		mqttSubscribe(t, subs[i], 1, "broadcast", 0)
	}

	time.Sleep(50 * time.Millisecond)

	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "multi-pub", true)
	mqttPublishQoS0(t, pub, "broadcast", []byte("msg"))

	var wg sync.WaitGroup
	for i := range subs {
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			pkt := mqttReadPublish(t, conn, 2*time.Second)
			if string(pkt.Payload) != "msg" {
				t.Errorf("Payload = %q, want %q", pkt.Payload, "msg")
			}
		}(subs[i])
	}
	wg.Wait()
}

func TestBroker_Pingreq(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	conn := dial(t, b.Addr())
	defer conn.Close()
	mqttConnect(t, conn, "ping-client", true)

	conn.Write([]byte{0xC0, 0x00})

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, _, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading PINGRESP: %v", err)
	}
	if fh.PacketType != codec.PINGRESP {
		t.Errorf("expected PINGRESP, got %s", codec.PacketTypeName(fh.PacketType))
	}
}

func TestBroker_RetainMessage(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "retain-pub", true)

	retainPkt := &codec.PublishPacket{
		Topic:   "status/online",
		QoS:     0,
		Retain:  true,
		Payload: []byte("true"),
	}
	pub.Write(retainPkt.Encode())

	time.Sleep(100 * time.Millisecond)

	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "retain-sub", true)
	mqttSubscribe(t, sub, 1, "status/online", 0)

	pkt := mqttReadPublish(t, sub, 2*time.Second)
	if string(pkt.Payload) != "true" {
		t.Errorf("Retained payload = %q, want %q", pkt.Payload, "true")
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	return conn
}

func waitForBroker(t *testing.T, b *Broker) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := b.Addr(); addr != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("broker did not start in time")
}
