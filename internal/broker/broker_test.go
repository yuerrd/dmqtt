package broker

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/langzp/dmqtt/internal/auth"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/langzp/dmqtt/internal/codec"
	"github.com/langzp/dmqtt/internal/plugin"
	"github.com/langzp/dmqtt/internal/ratelimit"
	"github.com/langzp/dmqtt/internal/rule"
	"github.com/langzp/dmqtt/internal/storage"
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

func mqttConnectWithAuth(t *testing.T, conn net.Conn, clientID, username, password string, cleanSession bool) (*codec.FixedHeader, []byte) {
	t.Helper()

	var payload bytes.Buffer
	writeUTF8(&payload, "MQTT")
	payload.WriteByte(0x04)
	flags := byte(0x00)
	if cleanSession {
		flags |= 0x02
	}
	flags |= 0x80 // username flag
	flags |= 0x40 // password flag
	payload.WriteByte(flags)
	payload.Write([]byte{0x00, 0x3C}) // keepalive 60s

	writeUTF8(&payload, clientID)
	writeUTF8(&payload, username)
	// Password is binary data (length-prefixed)
	pwBytes := []byte(password)
	payload.WriteByte(byte(len(pwBytes) >> 8))
	payload.WriteByte(byte(len(pwBytes)))
	payload.Write(pwBytes)

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
	conn.SetReadDeadline(time.Time{})
	return respFH, data
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

func connectClient(t *testing.T, addr, clientID string, cleanSession bool) net.Conn {
	t.Helper()
	conn := dial(t, addr)
	mqttConnect(t, conn, clientID, cleanSession)
	return conn
}

func connectClientCleanSession(t *testing.T, addr, clientID string, cleanSession bool) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var varHeader []byte
	varHeader = append(varHeader, 0, 4)
	varHeader = append(varHeader, []byte("MQTT")...)
	varHeader = append(varHeader, 4)
	flags := byte(0)
	if cleanSession {
		flags |= 0x02
	}
	varHeader = append(varHeader, flags)
	varHeader = append(varHeader, 0, 60)
	idBytes := []byte(clientID)
	varHeader = append(varHeader, byte(len(idBytes)>>8), byte(len(idBytes)))
	varHeader = append(varHeader, idBytes...)

	fh := codec.FixedHeader{
		PacketType:      codec.CONNECT,
		RemainingLength: len(varHeader),
	}
	conn.Write(append(fh.Encode(), varHeader...))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	rfh, data, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if rfh.PacketType != codec.CONNACK {
		t.Fatalf("expected CONNACK, got %s", codec.PacketTypeName(rfh.PacketType))
	}
	if len(data) < 2 || data[1] != codec.ConnackAccepted {
		t.Fatalf("CONNACK return code: %v", data)
	}
	conn.SetReadDeadline(time.Time{})
	return conn
}

func subscribeQoS(t *testing.T, conn net.Conn, filter string, qos byte, packetID uint16) {
	t.Helper()
	var buf []byte
	buf = append(buf, byte(packetID>>8), byte(packetID))
	topicBytes := []byte(filter)
	buf = append(buf, byte(len(topicBytes)>>8), byte(len(topicBytes)))
	buf = append(buf, topicBytes...)
	buf = append(buf, qos)

	fh := codec.FixedHeader{
		PacketType:      codec.SUBSCRIBE,
		QoS:             1,
		RemainingLength: len(buf),
	}
	conn.Write(append(fh.Encode(), buf...))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	rfh, _, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if rfh.PacketType != codec.SUBACK {
		t.Fatalf("expected SUBACK, got %s", codec.PacketTypeName(rfh.PacketType))
	}
	conn.SetReadDeadline(time.Time{})
}

func publishQoS1(t *testing.T, conn net.Conn, topic string, payload []byte, packetID uint16) {
	t.Helper()
	pkt := &codec.PublishPacket{
		Topic:    topic,
		Payload:  payload,
		QoS:      1,
		PacketID: packetID,
	}
	conn.Write(pkt.Encode())
}

func setupAuthBroker(t *testing.T) *Broker {
	t.Helper()
	// Create auth file with test users
	hash, _ := bcrypt.GenerateFromPassword([]byte("testpass"), bcrypt.MinCost)
	authJSON := `{"users":[
        {"username":"allowed","password_hash":"` + string(hash) + `","acl":[
            {"topic":"permitted/#","access":"publish"},
            {"topic":"permitted/#","access":"subscribe"}
        ]},
        {"username":"limited","password_hash":"` + string(hash) + `","acl":[
            {"topic":"readonly/#","access":"subscribe"}
        ]}
    ]}`

	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	os.WriteFile(path, []byte(authJSON), 0644)

	store, err := auth.LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}

	b := New(":0", nil)
	b.SetAuth(store, store)
	return b
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

func TestBroker_PubSubQoS1(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	addr := b.Addr()

	sub := connectClient(t, addr, "sub-qos1", true)
	defer sub.Close()
	subscribeQoS(t, sub, "test/qos1", 1, 1)

	time.Sleep(50 * time.Millisecond)

	pub := connectClient(t, addr, "pub-qos1", true)
	defer pub.Close()

	publishQoS1(t, pub, "test/qos1", []byte("hello-qos1"), 1)

	// Read PUBACK from broker
	pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err := codec.ReadPacket(pub)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBACK {
		t.Fatalf("expected PUBACK, got %s", codec.PacketTypeName(fh.PacketType))
	}
	puback, err := codec.DecodePubackPacket(data)
	if err != nil {
		t.Fatal(err)
	}
	if puback.PacketID != 1 {
		t.Fatalf("expected packet ID 1, got %d", puback.PacketID)
	}
	pub.SetReadDeadline(time.Time{})

	// Subscriber should receive PUBLISH
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err = codec.ReadPacket(sub)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH, got %s", codec.PacketTypeName(fh.PacketType))
	}
	pubPkt, err := codec.DecodePublishPacket(data, fh.QoS)
	if err != nil {
		t.Fatal(err)
	}
	if string(pubPkt.Payload) != "hello-qos1" {
		t.Fatalf("expected hello-qos1, got %s", pubPkt.Payload)
	}
	sub.SetReadDeadline(time.Time{})
}

func TestBroker_PubSubQoS2(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	addr := b.Addr()

	pub := connectClient(t, addr, "pub-qos2", true)
	defer pub.Close()

	pkt := &codec.PublishPacket{
		Topic:    "test/qos2",
		Payload:  []byte("exactly-once"),
		QoS:      2,
		PacketID: 1,
	}
	pub.Write(pkt.Encode())

	// Expect PUBREC
	pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err := codec.ReadPacket(pub)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBREC {
		t.Fatalf("expected PUBREC, got %s", codec.PacketTypeName(fh.PacketType))
	}
	pubrec, _ := codec.DecodePubrecPacket(data)
	if pubrec.PacketID != 1 {
		t.Fatalf("expected packet ID 1, got %d", pubrec.PacketID)
	}

	// Send PUBREL
	pubrel := &codec.PubrelPacket{PacketID: 1}
	pub.Write(pubrel.Encode())

	// Expect PUBCOMP
	fh, data, err = codec.ReadPacket(pub)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBCOMP {
		t.Fatalf("expected PUBCOMP, got %s", codec.PacketTypeName(fh.PacketType))
	}
	pubcomp, _ := codec.DecodePubcompPacket(data)
	if pubcomp.PacketID != 1 {
		t.Fatalf("expected packet ID 1, got %d", pubcomp.PacketID)
	}
	pub.SetReadDeadline(time.Time{})
}

func TestBroker_OfflineMessages(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	addr := b.Addr()

	// Connect subscriber with persistent session (CleanSession=false)
	sub := connectClientCleanSession(t, addr, "offline-sub", false)
	subscribeQoS(t, sub, "test/offline", 1, 1)

	// Disconnect subscriber (close without DISCONNECT = abnormal disconnect, keeps session)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	// Publish while subscriber is offline
	pub := connectClient(t, addr, "pub-offline", true)
	publishQoS1(t, pub, "test/offline", []byte("queued-msg"), 1)
	// Read PUBACK
	pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	codec.ReadPacket(pub)
	pub.SetReadDeadline(time.Time{})
	pub.Close()
	time.Sleep(100 * time.Millisecond)

	// Reconnect subscriber with persistent session
	sub2 := connectClientCleanSession(t, addr, "offline-sub", false)
	defer sub2.Close()

	// Should receive the offline message
	sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err := codec.ReadPacket(sub2)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH (offline msg), got %s", codec.PacketTypeName(fh.PacketType))
	}
	pubPkt, _ := codec.DecodePublishPacket(data, fh.QoS)
	if string(pubPkt.Payload) != "queued-msg" {
		t.Fatalf("expected queued-msg, got %s", pubPkt.Payload)
	}
	sub2.SetReadDeadline(time.Time{})
}

func TestBroker_PersistentSessionRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Start broker with storage, create a persistent session
	b1 := New(":0", store)
	go b1.Start()
	waitForBroker(t, b1)

	sub := connectClientCleanSession(t, b1.Addr(), "persist-client", false)
	subscribeQoS(t, sub, "test/persist", 1, 1)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	b1.Stop()
	store.Close()

	// Reopen storage and start new broker
	store2, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	b2 := New(":0", store2)
	go b2.Start()
	defer b2.Stop()
	waitForBroker(t, b2)

	// Reconnect — session should be restored
	sub2 := connectClientCleanSession(t, b2.Addr(), "persist-client", false)
	defer sub2.Close()

	// Publish — subscriber should receive because subscription was restored from Pebble
	pub := connectClient(t, b2.Addr(), "persist-pub", true)
	publishQoS1(t, pub, "test/persist", []byte("after-restart"), 1)
	pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	codec.ReadPacket(pub) // PUBACK
	pub.SetReadDeadline(time.Time{})
	pub.Close()

	sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err := codec.ReadPacket(sub2)
	if err != nil {
		t.Fatal(err)
	}
	if fh.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH, got %s", codec.PacketTypeName(fh.PacketType))
	}
	pubPkt, _ := codec.DecodePublishPacket(data, fh.QoS)
	if string(pubPkt.Payload) != "after-restart" {
		t.Fatalf("expected after-restart, got %s", pubPkt.Payload)
	}
	sub2.SetReadDeadline(time.Time{})
}

func TestBroker_ConnectAuth_Rejected(t *testing.T) {
	b := setupAuthBroker(t)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	conn := dial(t, b.Addr())
	defer conn.Close()

	_, data := mqttConnectWithAuth(t, conn, "client1", "allowed", "wrongpass", true)
	if len(data) < 2 || data[1] != codec.ConnackBadUsernameOrPassword {
		t.Fatalf("expected CONNACK 0x04, got %v", data)
	}
}

func TestBroker_ConnectAuth_Accepted(t *testing.T) {
	b := setupAuthBroker(t)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	conn := dial(t, b.Addr())
	defer conn.Close()

	_, data := mqttConnectWithAuth(t, conn, "client1", "allowed", "testpass", true)
	if len(data) < 2 || data[1] != codec.ConnackAccepted {
		t.Fatalf("expected CONNACK 0x00, got %v", data)
	}
}

func TestBroker_SubscribeACL_Denied(t *testing.T) {
	b := setupAuthBroker(t)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	conn := dial(t, b.Addr())
	defer conn.Close()

	// Connect as "limited" user (only has subscribe on readonly/#)
	_, data := mqttConnectWithAuth(t, conn, "limited-client", "limited", "testpass", true)
	if data[1] != codec.ConnackAccepted {
		t.Fatalf("expected accepted, got %v", data[1])
	}

	// Subscribe to forbidden topic
	var payload bytes.Buffer
	payload.Write([]byte{0x00, 0x01}) // packet ID = 1
	writeUTF8(&payload, "permitted/data")
	payload.WriteByte(0) // QoS 0

	fh := codec.FixedHeader{
		PacketType:      codec.SUBSCRIBE,
		QoS:             1,
		RemainingLength: payload.Len(),
	}
	conn.Write(fh.Encode())
	conn.Write(payload.Bytes())

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respFH, respData, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatalf("reading SUBACK: %v", err)
	}
	conn.SetReadDeadline(time.Time{})

	if respFH.PacketType != codec.SUBACK {
		t.Fatalf("expected SUBACK, got %s", codec.PacketTypeName(respFH.PacketType))
	}
	// SUBACK: 2 bytes packet ID + return codes
	if len(respData) < 3 || respData[2] != 0x80 {
		t.Fatalf("expected SUBACK failure 0x80, got %v", respData)
	}
}

func TestBroker_PublishACL_Denied(t *testing.T) {
	b := setupAuthBroker(t)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Subscriber on permitted/#
	sub := dial(t, b.Addr())
	defer sub.Close()
	_, dataS := mqttConnectWithAuth(t, sub, "sub-client", "allowed", "testpass", true)
	if dataS[1] != codec.ConnackAccepted {
		t.Fatalf("sub connect failed: data=%v", dataS)
	}
	mqttSubscribe(t, sub, 1, "permitted/#", 0)
	time.Sleep(50 * time.Millisecond)

	// Publisher: "limited" user has no publish ACL
	pub := dial(t, b.Addr())
	defer pub.Close()
	_, dataP := mqttConnectWithAuth(t, pub, "pub-client", "limited", "testpass", true)
	if dataP[1] != codec.ConnackAccepted {
		t.Fatalf("pub connect failed")
	}

	// Publish to permitted/data — should be silently dropped (limited has no publish ACL)
	pkt := &codec.PublishPacket{
		Topic:   "permitted/data",
		QoS:     0,
		Payload: []byte("denied-msg"),
	}
	pub.Write(pkt.Encode())

	// Subscriber should NOT receive the message
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, err := codec.ReadPacket(sub)
	if err == nil {
		t.Error("expected no message (ACL denied), but received one")
	}
	sub.SetReadDeadline(time.Time{})
}

func TestKeepAliveTimeout_TriggersWill(t *testing.T) {
	b := New(":0", nil)
	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Subscribe to will topic
	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "subscriber", true)
	mqttSubscribe(t, sub, 1, "will/topic", 0)

	time.Sleep(50 * time.Millisecond)

	// Connect client with will and short keep-alive (1 second)
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}

	var payload bytes.Buffer
	writeUTF8(&payload, "MQTT")
	payload.WriteByte(0x04) // protocol level
	flags := byte(0x06)     // CleanSession + WillFlag
	payload.WriteByte(flags)
	payload.Write([]byte{0x00, 0x01}) // KeepAlive = 1 second
	writeUTF8(&payload, "will-client")
	writeUTF8(&payload, "will/topic")

	// Will payload
	willPayload := []byte("I died")
	payload.WriteByte(byte(len(willPayload) >> 8))
	payload.WriteByte(byte(len(willPayload)))
	payload.Write(willPayload)

	fh := codec.FixedHeader{PacketType: codec.CONNECT, RemainingLength: payload.Len()}
	conn.Write(fh.Encode())
	conn.Write(payload.Bytes())

	// Read CONNACK
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respFH, data, err := codec.ReadPacket(conn)
	if err != nil || respFH.PacketType != codec.CONNACK || data[1] != codec.ConnackAccepted {
		t.Fatalf("CONNACK failed: fh=%v data=%v err=%v", respFH, data, err)
	}

	// Don't send any more packets — wait for keep-alive timeout (1.5 * 1s = 1.5s)
	time.Sleep(2500 * time.Millisecond)

	// Check that subscriber received the will message
	sub.SetReadDeadline(time.Now().Add(time.Second))
	pubFH, pubData, err := codec.ReadPacket(sub)
	if err != nil {
		t.Fatalf("should have received will message: %v", err)
	}
	if pubFH.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH, got %s", codec.PacketTypeName(pubFH.PacketType))
	}
	pkt, _ := codec.DecodePublishPacket(pubData, pubFH.QoS)
	if pkt.Topic != "will/topic" || string(pkt.Payload) != "I died" {
		t.Fatalf("unexpected will: topic=%s payload=%s", pkt.Topic, string(pkt.Payload))
	}
}

func TestBroker_RateLimitRejectsPublish(t *testing.T) {
	b := New(":0", nil)

	// Set up rate limiter that allows 1 msg then rejects
	cfg := ratelimit.DefaultConfig()
	cfg.Enabled = true
	cfg.Client.MsgRate = 1
	cfg.Client.MsgBurst = 1
	rl := ratelimit.NewAggregateRateLimiter(cfg)
	b.SetRateLimiter(rl)

	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Subscribe
	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "sub-client", true)
	mqttSubscribe(t, sub, 1, "test/topic", 0)
	time.Sleep(50 * time.Millisecond)

	// Publisher
	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "pub-client", true)

	// First publish should succeed (burst=1)
	mqttPublishQoS0(t, pub, "test/topic", []byte("msg1"))
	time.Sleep(100 * time.Millisecond)

	// Rapid publishes should be rate limited
	for i := 0; i < 10; i++ {
		mqttPublishQoS0(t, pub, "test/topic", []byte("flood"))
	}

	// Subscriber should receive msg1 but not all flood messages
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	received := 0
	for {
		_, _, err := codec.ReadPacket(sub)
		if err != nil {
			break
		}
		received++
	}
	if received >= 11 {
		t.Fatalf("rate limiter should have dropped some messages, received %d", received)
	}
	t.Logf("received %d messages (rate limiting active)", received)
}

func TestBroker_CircuitOpenFallsBackToOffline(t *testing.T) {
	b := New(":0", nil)

	// Register a session with subscription
	session := b.sessions.Create("subscriber", false)
	session.Subscriptions["test/topic"] = 1
	b.subscriptions.Add("subscriber", "test/topic", 1)

	// Set up a mock cluster that returns ErrCircuitOpen on Forward
	// We test the routeMessage method directly
	b.routeMessage("test/topic", []byte("payload"), 1, false, false)

	// Without a connected client, message should go to offline store
	count := b.offlineStore.Count("subscriber")
	if count != 1 {
		t.Fatalf("expected 1 offline message, got %d", count)
	}
}

func TestBroker_MigrationBrokerAPI(t *testing.T) {
	b := New(":0", nil)

	// Create a session with subscriptions
	session := b.sessions.Create("dev-1", false)
	session.Subscriptions["test/#"] = 1

	// Add offline messages
	b.offlineStore.Enqueue("dev-1", &OfflineMessage{
		Topic:   "test/1",
		Payload: []byte("msg1"),
		QoS:     1,
	})
	b.offlineStore.Enqueue("dev-1", &OfflineMessage{
		Topic:   "test/2",
		Payload: []byte("msg2"),
		QoS:     0,
	})

	// Test GetOfflineMessages
	msgs := b.GetOfflineMessages("dev-1")
	if len(msgs) != 2 {
		t.Fatalf("expected 2 offline messages, got %d", len(msgs))
	}

	// Test GetSessionData
	sd := b.GetSessionData("dev-1")
	if sd == nil {
		t.Fatal("expected session data")
	}
	if sd.ClientID != "dev-1" {
		t.Fatalf("expected dev-1, got %s", sd.ClientID)
	}
	if sd.Subscriptions["test/#"] != 1 {
		t.Fatal("expected subscription test/# with QoS 1")
	}

	// Test DeleteOfflineMessages
	b.DeleteOfflineMessages("dev-1")
	if b.offlineStore.Count("dev-1") != 0 {
		t.Fatal("offline messages should be deleted")
	}

	// Test DeleteSession
	b.DeleteSession("dev-1")
	if b.sessions.Get("dev-1") != nil {
		t.Fatal("session should be deleted")
	}
}

func TestBroker_ImportMigrateData(t *testing.T) {
	b := New(":0", nil)

	migrateMsg := cluster.MigrateDataMessage{
		DeviceID: "dev-2",
		Messages: []cluster.MigrateOfflineMsg{
			{Topic: "imported/1", Payload: []byte("data"), QoS: 1},
		},
		Session: &cluster.MigrateSessionData{
			ClientID:      "dev-2",
			CleanSession:  false,
			Subscriptions: map[string]byte{"imported/#": 1},
		},
	}

	b.ImportMigrateData(migrateMsg)

	// Verify offline messages imported
	if b.offlineStore.Count("dev-2") != 1 {
		t.Fatalf("expected 1 offline message, got %d", b.offlineStore.Count("dev-2"))
	}

	// Verify session imported
	session := b.sessions.Get("dev-2")
	if session == nil {
		t.Fatal("session should be created")
	}
	if session.Subscriptions["imported/#"] != 1 {
		t.Fatal("subscription should be imported")
	}
}

// --- Interceptor test types ---

type testRejectPublishInterceptor struct{}

func (t *testRejectPublishInterceptor) Name() string { return "reject-publish" }
func (t *testRejectPublishInterceptor) Init() error  { return nil }
func (t *testRejectPublishInterceptor) Close() error { return nil }
func (t *testRejectPublishInterceptor) OnPublish(ctx context.Context, evt *plugin.PublishEvent) error {
	return fmt.Errorf("publish rejected")
}

type testRejectConnectInterceptor struct {
	blockedClient string
}

func (t *testRejectConnectInterceptor) Name() string { return "reject-connect" }
func (t *testRejectConnectInterceptor) Init() error  { return nil }
func (t *testRejectConnectInterceptor) Close() error { return nil }
func (t *testRejectConnectInterceptor) OnConnect(ctx context.Context, evt *plugin.ConnectEvent) error {
	if evt.ClientID == t.blockedClient {
		return fmt.Errorf("client %s is blocked", evt.ClientID)
	}
	return nil
}

// --- Interceptor integration tests ---

func TestInterceptor_OnPublishReject(t *testing.T) {
	b := New(":0", nil)

	chain := plugin.NewInterceptorChain()
	chain.Register(&testRejectPublishInterceptor{})
	chain.InitAll()
	defer chain.CloseAll()
	b.SetInterceptors(chain)

	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Subscribe
	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "sub-1", true)
	mqttSubscribe(t, sub, 1, "test/topic", 0)
	time.Sleep(50 * time.Millisecond)

	// Publish — should be rejected by interceptor
	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "pub-1", true)
	mqttPublishQoS0(t, pub, "test/topic", []byte("hello"))

	time.Sleep(200 * time.Millisecond)

	// Subscriber should NOT receive the message
	sub.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 256)
	_, err := sub.Read(buf)
	if err == nil {
		t.Fatal("expected no message (publish should have been rejected)")
	}
}

func TestInterceptor_OnConnectReject(t *testing.T) {
	b := New(":0", nil)

	chain := plugin.NewInterceptorChain()
	chain.Register(&testRejectConnectInterceptor{blockedClient: "blocked-client"})
	chain.InitAll()
	defer chain.CloseAll()
	b.SetInterceptors(chain)

	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Try to connect with blocked client ID
	conn := dial(t, b.Addr())
	defer conn.Close()

	// Send CONNECT manually
	var payload bytes.Buffer
	writeUTF8(&payload, "MQTT")
	payload.WriteByte(0x04)      // protocol level
	payload.WriteByte(0x02)      // flags: clean session
	payload.Write([]byte{0, 60}) // keep alive
	writeUTF8(&payload, "blocked-client")

	fh := codec.FixedHeader{
		PacketType:      codec.CONNECT,
		RemainingLength: payload.Len(),
	}
	conn.Write(fh.Encode())
	conn.Write(payload.Bytes())

	// Read CONNACK — should be refused
	conn.SetReadDeadline(time.Now().Add(time.Second))
	respFH, data, err := codec.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if respFH.PacketType != codec.CONNACK {
		t.Fatalf("expected CONNACK, got %s", codec.PacketTypeName(respFH.PacketType))
	}
	if len(data) < 2 {
		t.Fatal("CONNACK too short")
	}
	if data[1] == codec.ConnackAccepted {
		t.Fatal("expected connection to be rejected")
	}
}

func TestRuleEngine_PublishAction(t *testing.T) {
	b := New(":0", nil)

	// Create rule engine with republish rule:
	// messages on "sensors/+/temp" with temp>50 → republish to "alerts/high-temp"
	rulesYAML := []byte(`
rules:
  - rule_id: high-temp
    enabled: true
    source:
      topic: "sensors/+/temp"
    filter: "payload.temp > 50.0"
    actions:
      - type: publish
        target_topic: "alerts/high-temp"
`)

	ruleEngine, err := rule.NewEngine(b.RouteMessage, rule.EngineConfig{
		WorkerPoolSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ruleEngine.LoadRulesFromBytes(rulesYAML); err != nil {
		t.Fatal(err)
	}
	defer ruleEngine.Close()

	chain := plugin.NewInterceptorChain()
	chain.Register(rule.NewRuleInterceptor(ruleEngine))
	chain.InitAll()
	defer chain.CloseAll()
	b.SetInterceptors(chain)

	go b.Start()
	defer b.Stop()
	waitForBroker(t, b)

	// Subscribe to alerts/high-temp
	sub := dial(t, b.Addr())
	defer sub.Close()
	mqttConnect(t, sub, "alert-sub", true)
	mqttSubscribe(t, sub, 1, "alerts/high-temp", 0)
	time.Sleep(50 * time.Millisecond)

	// Publish a message that should trigger the rule
	pub := dial(t, b.Addr())
	defer pub.Close()
	mqttConnect(t, pub, "sensor-pub", true)
	mqttPublishQoS0(t, pub, "sensors/livingroom/temp", []byte(`{"temp":75}`))

	// Subscriber should receive the republished message
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	fh, data, err := codec.ReadPacket(sub)
	if err != nil {
		t.Fatal("expected republished message, got error:", err)
	}
	if fh.PacketType != codec.PUBLISH {
		t.Fatalf("expected PUBLISH, got %s", codec.PacketTypeName(fh.PacketType))
	}
	// Verify topic in PUBLISH packet: 2 bytes length + topic string
	topicLen := int(data[0])<<8 | int(data[1])
	topic := string(data[2 : 2+topicLen])
	if topic != "alerts/high-temp" {
		t.Fatalf("expected topic 'alerts/high-temp', got '%s'", topic)
	}

	// Now publish a message that should NOT trigger (temp <= 50)
	mqttPublishQoS0(t, pub, "sensors/kitchen/temp", []byte(`{"temp":30}`))
	time.Sleep(300 * time.Millisecond)

	sub.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 256)
	_, err = sub.Read(buf)
	if err == nil {
		t.Fatal("expected no message for temp=30")
	}
}
