package cluster

import (
	"sync"
	"testing"
	"time"
)

// mockBrokerAPI implements MigrationBrokerAPI for testing
type mockBrokerAPI struct {
	mu           sync.Mutex
	disconnected []string
	offlineMsgs  map[string][]MigrateOfflineMsg
	sessions     map[string]*MigrateSessionData
	imported     []MigrateDataMessage
}

func newMockBrokerAPI() *mockBrokerAPI {
	return &mockBrokerAPI{
		offlineMsgs: make(map[string][]MigrateOfflineMsg),
		sessions:    make(map[string]*MigrateSessionData),
	}
}

func (m *mockBrokerAPI) DisconnectDevice(deviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnected = append(m.disconnected, deviceID)
}

func (m *mockBrokerAPI) GetOfflineMessages(deviceID string) []MigrateOfflineMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.offlineMsgs[deviceID]
}

func (m *mockBrokerAPI) DeleteOfflineMessages(deviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.offlineMsgs, deviceID)
}

func (m *mockBrokerAPI) GetSessionData(clientID string) *MigrateSessionData {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[clientID]
}

func (m *mockBrokerAPI) DeleteSession(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, clientID)
}

func (m *mockBrokerAPI) ImportMigrateData(msg MigrateDataMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.imported = append(m.imported, msg)
}

func TestMigrationExecutor_ExecuteLocalMigration(t *testing.T) {
	broker := newMockBrokerAPI()
	broker.offlineMsgs["dev-1"] = []MigrateOfflineMsg{
		{Topic: "t/1", Payload: []byte("p1"), QoS: 1},
	}
	broker.sessions["dev-1"] = &MigrateSessionData{
		ClientID:      "dev-1",
		CleanSession:  false,
		Subscriptions: map[string]byte{"t/#": 1},
	}

	// Create two PeerTransport instances to simulate inter-node communication
	pt1, err := NewPeerTransport("127.0.0.1:0", "node-1", func(msg ForwardMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	defer pt1.Stop()

	pt2, err := NewPeerTransport("127.0.0.1:0", "node-2", func(msg ForwardMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	pt2.SetMigrateHandler(func(msg MigrateDataMessage) {
		broker.ImportMigrateData(msg)
	})
	defer pt2.Stop()

	// Connect pt1 -> pt2
	pt1.AddPeer("node-2", pt2.listenAddr)
	time.Sleep(200 * time.Millisecond)

	connIdx := NewConnectionIndex()
	connIdx.Add("dev-1", "node-1")

	executor := NewMigrationExecutor(pt1, connIdx, broker)

	migration := NewShardMigration("mig-1", "node-1", "node-2",
		[]string{"dev-1"}, MigrationConfig{
			BatchSize:     10,
			BatchInterval: 10 * time.Millisecond,
			MaxRetries:    1,
		})

	err = executor.Execute(migration)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if migration.Status != MigrationCompleted {
		t.Fatalf("expected completed, got %v (error: %s)", migration.Status, migration.Error)
	}
	if migration.Progress.Migrated != 1 {
		t.Fatalf("expected 1 migrated, got %d", migration.Progress.Migrated)
	}

	// Verify device was disconnected
	broker.mu.Lock()
	if len(broker.disconnected) != 1 || broker.disconnected[0] != "dev-1" {
		t.Fatalf("expected dev-1 disconnected, got %v", broker.disconnected)
	}
	// Verify data was imported on receiver side
	if len(broker.imported) != 1 {
		t.Fatalf("expected 1 imported, got %d", len(broker.imported))
	}
	broker.mu.Unlock()

	// Verify offline data cleaned up on source
	if len(broker.offlineMsgs["dev-1"]) != 0 {
		t.Fatal("source offline messages should be deleted")
	}
	if broker.sessions["dev-1"] != nil {
		t.Fatal("source session should be deleted")
	}
}
