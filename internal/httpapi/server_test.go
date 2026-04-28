package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yuerrd/dmqtt/internal/broker"
	"github.com/yuerrd/dmqtt/internal/cluster"
)

type mockChecker struct {
	ready bool
}

func (m *mockChecker) IsReady() bool { return m.ready }

func TestHealth(t *testing.T) {
	checker := &mockChecker{ready: false}
	srv := New(":0", checker)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/health", srv.Addr())

	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("status = %q, want ok", body["status"])
	}
}

func TestReady_NotReady(t *testing.T) {
	checker := &mockChecker{ready: false}
	srv := New(":0", checker)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/ready", srv.Addr())

	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "not_ready" {
		t.Errorf("status = %q, want not_ready", body["status"])
	}
}

func TestReady_Ready(t *testing.T) {
	checker := &mockChecker{ready: true}
	srv := New(":0", checker)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/ready", srv.Addr())

	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ready" {
		t.Errorf("status = %q, want ready", body["status"])
	}
}

func TestMetricsEndpoint(t *testing.T) {
	checker := &mockChecker{ready: true}
	srv := New(":0", checker)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/metrics", srv.Addr())

	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); ct == "" {
		t.Error("Content-Type header missing")
	}
}

type mockBrokerAPI struct {
	clientIDs   []string
	clientInfos map[string]*broker.ClientInfo
	sessionData map[string]*cluster.MigrateSessionData
}

func (m *mockBrokerAPI) ConnectedClientIDs() []string         { return m.clientIDs }
func (m *mockBrokerAPI) GetClientInfo(id string) *broker.ClientInfo {
	if m.clientInfos != nil {
		return m.clientInfos[id]
	}
	return nil
}
func (m *mockBrokerAPI) GetSessionData(id string) *cluster.MigrateSessionData {
	if m.sessionData != nil {
		return m.sessionData[id]
	}
	return nil
}
func (m *mockBrokerAPI) ClientCount() int              { return len(m.clientIDs) }
func (m *mockBrokerAPI) ActiveSubscriptions() int      { return 42 }
func (m *mockBrokerAPI) RetainedMessageCount() int     { return 5 }
func (m *mockBrokerAPI) ClusterNodeCount() int         { return 3 }
func (m *mockBrokerAPI) DisconnectDevice(id string)    {}

func newTestMock() *mockBrokerAPI {
	return &mockBrokerAPI{
		clientIDs: []string{"dev-a", "dev-b", "dev-c"},
		clientInfos: map[string]*broker.ClientInfo{
			"dev-a": {
				ClientID:        "dev-a",
				Username:        "user1",
				RemoteAddr:      "10.0.0.1:5000",
				ProtocolVersion: 4,
				ConnectedAt:     time.Date(2026, 4, 18, 10, 0, 0, 0, time.UTC),
				KeepAlive:       60 * time.Second,
			},
			"dev-b": {
				ClientID:        "dev-b",
				Username:        "user2",
				RemoteAddr:      "10.0.0.2:5001",
				ProtocolVersion: 5,
				ConnectedAt:     time.Date(2026, 4, 18, 11, 0, 0, 0, time.UTC),
				KeepAlive:       120 * time.Second,
			},
			"dev-c": {
				ClientID:        "dev-c",
				Username:        "",
				RemoteAddr:      "10.0.0.3:5002",
				ProtocolVersion: 4,
				ConnectedAt:     time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC),
				KeepAlive:       0,
			},
		},
		sessionData: map[string]*cluster.MigrateSessionData{
			"dev-a": {
				ClientID:       "dev-a",
				CleanStart:     false,
				ExpiryInterval: 3600,
				Subscriptions:  map[string]byte{"sensor/#": 1},
			},
		},
	}
}

func TestDevicesList(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	total := int(body["total"].(float64))
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	devices := body["devices"].([]interface{})
	if len(devices) != 3 {
		t.Errorf("devices count = %d, want 3", len(devices))
	}
}

func TestDevicesList_Pagination(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices?page=1&per_page=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	devices := body["devices"].([]interface{})
	if len(devices) != 2 {
		t.Errorf("page 1 devices = %d, want 2", len(devices))
	}
	if int(body["total"].(float64)) != 3 {
		t.Errorf("total = %v, want 3", body["total"])
	}

	// Page 2
	resp2, err := http.Get("http://" + srv.Addr() + "/api/v1/devices?page=2&per_page=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	var body2 map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&body2)
	devices2 := body2["devices"].([]interface{})
	if len(devices2) != 1 {
		t.Errorf("page 2 devices = %d, want 1", len(devices2))
	}
}

func TestDeviceDetail(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices/dev-a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["client_id"] != "dev-a" {
		t.Errorf("client_id = %v, want dev-a", body["client_id"])
	}
	if body["connected"] != true {
		t.Error("connected should be true")
	}
	if body["username"] != "user1" {
		t.Errorf("username = %v, want user1", body["username"])
	}
	sess := body["session"].(map[string]interface{})
	if sess == nil {
		t.Fatal("session is nil")
	}
	subs := sess["subscriptions"].(map[string]interface{})
	if subs["sensor/#"] == nil {
		t.Error("missing subscription sensor/#")
	}
}

func TestDeviceDetail_NotFound(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices/unknown")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDeviceDetail_NoSession(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// dev-b has no session data
	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices/dev-b")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["client_id"] != "dev-b" {
		t.Errorf("client_id = %v, want dev-b", body["client_id"])
	}
	if body["session"] != nil {
		t.Error("expected no session for dev-b")
	}
}

func TestDeviceDisconnect(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("POST", "http://"+srv.Addr()+"/api/v1/devices/dev-a/disconnect", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "disconnected" {
		t.Errorf("status = %v, want disconnected", body["status"])
	}
}

func TestDeviceDisconnect_NotFound(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("POST", "http://"+srv.Addr()+"/api/v1/devices/unknown/disconnect", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestStatsEndpoint(t *testing.T) {
	mock := &mockBrokerAPI{clientIDs: []string{"c1", "c2"}}
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]float64
	json.NewDecoder(resp.Body).Decode(&body)
	if body["connected_clients"] != 2 {
		t.Errorf("connected_clients = %v, want 2", body["connected_clients"])
	}
	if body["active_subscriptions"] != 42 {
		t.Errorf("active_subscriptions = %v, want 42", body["active_subscriptions"])
	}
	if body["retained_messages"] != 5 {
		t.Errorf("retained_messages = %v, want 5", body["retained_messages"])
	}
	if body["cluster_nodes"] != 3 {
		t.Errorf("cluster_nodes = %v, want 3", body["cluster_nodes"])
	}
}

func TestNodesEndpoint_NoClusters(t *testing.T) {
	srv := New(":0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 0 {
		t.Errorf("nodes = %d, want 0", len(nodes))
	}
}

func TestMigrationAPI_ListEmpty(t *testing.T) {
	coord := cluster.NewMigrationCoordinator(3)
	srv := New(":0", nil)
	srv.SetMigrationCoordinator(coord)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/migrations")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result []interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected empty list, got %d", len(result))
	}
}

func TestMigrationAPI_GetNotFound(t *testing.T) {
	coord := cluster.NewMigrationCoordinator(3)
	srv := New(":0", nil)
	srv.SetMigrationCoordinator(coord)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/migrations/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestMigrationAPI_CancelNotFound(t *testing.T) {
	coord := cluster.NewMigrationCoordinator(3)
	srv := New(":0", nil)
	srv.SetMigrationCoordinator(coord)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("POST", "http://"+srv.Addr()+"/api/v1/cluster/migrations/nonexistent/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestAdminEndpoint(t *testing.T) {
	checker := &mockChecker{ready: true}
	srv := New(":0", checker)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/admin/", srv.Addr())
	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /admin/: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", contentType)
	}
}

func TestStats_Enhanced(t *testing.T) {
	checker := &mockChecker{ready: true}
	srv := New(":0", checker)
	srv.SetBrokerAPI(&mockBrokerAPI{
		clientIDs: []string{"c1", "c2"},
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	url := fmt.Sprintf("http://%s/api/v1/stats", srv.Addr())
	var resp *http.Response
	var err error
	for i := 0; i < 10; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /api/v1/stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if int(body["connected_clients"].(float64)) != 2 {
		t.Errorf("connected_clients = %v, want 2", body["connected_clients"])
	}

	requiredFields := []string{
		"uptime_seconds", "goroutines",
		"memory_alloc_bytes", "memory_sys_bytes",
		"gc_pause_total_ns",
	}
	for _, f := range requiredFields {
		if _, ok := body[f]; !ok {
			t.Errorf("missing field %q in /stats response", f)
		}
	}
}

// mockClusterAPI implements ClusterAPI for tests.
type mockClusterAPI struct {
	self    cluster.NodeInfo
	members []cluster.NodeInfo
}

func (m *mockClusterAPI) Self() cluster.NodeInfo    { return m.self }
func (m *mockClusterAPI) Members() []cluster.NodeInfo { return m.members }

func TestNodesEndpoint_WithCluster(t *testing.T) {
	mock := &mockClusterAPI{
		self: cluster.NodeInfo{ID: "node-1", Host: "localhost", GossipPort: 7946},
		members: []cluster.NodeInfo{
			{ID: "node-1", Host: "localhost", GossipPort: 7946},
			{ID: "node-2", Host: "192.168.1.2", GossipPort: 7946},
		},
	}
	srv := New(":0", nil)
	srv.SetClusterAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["self"] != "node-1" {
		t.Errorf("self = %v, want node-1", body["self"])
	}
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 2 {
		t.Errorf("nodes = %d, want 2", len(nodes))
	}
}

func TestSetAPIKey_Unauthorized(t *testing.T) {
	srv := New(":0", nil)
	srv.SetAPIKey("secret-key")
	srv.SetBrokerAPI(&mockBrokerAPI{clientIDs: []string{"c1"}})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// No API key → 401
	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestSetAPIKey_QueryParam(t *testing.T) {
	srv := New(":0", nil)
	srv.SetAPIKey("secret-key")
	srv.SetBrokerAPI(&mockBrokerAPI{clientIDs: []string{"c1"}})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices?api_key=secret-key")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestSetAPIKey_Header(t *testing.T) {
	srv := New(":0", nil)
	srv.SetAPIKey("secret-key")
	srv.SetBrokerAPI(&mockBrokerAPI{clientIDs: []string{"c1"}})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("GET", "http://"+srv.Addr()+"/api/v1/devices", nil)
	req.Header.Set("X-API-Key", "secret-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestSetAPIKey_WrongKey(t *testing.T) {
	srv := New(":0", nil)
	srv.SetAPIKey("secret-key")
	srv.SetBrokerAPI(&mockBrokerAPI{clientIDs: []string{"c1"}})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("GET", "http://"+srv.Addr()+"/api/v1/devices", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestDevicesList_NoBrokerAPI(t *testing.T) {
	srv := New(":0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestDeviceDetail_NoBrokerAPI(t *testing.T) {
	srv := New(":0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/devices/unknown")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestMigrations_NoCoordinator(t *testing.T) {
	srv := New(":0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/migrations")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestMigrations_MethodNotAllowed(t *testing.T) {
	coord := cluster.NewMigrationCoordinator(3)
	srv := New(":0", nil)
	srv.SetMigrationCoordinator(coord)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	req, _ := http.NewRequest("DELETE", "http://"+srv.Addr()+"/api/v1/cluster/migrations", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestAddr_BeforeStart(t *testing.T) {
	srv := New(":0", nil)
	if addr := srv.Addr(); addr != "" {
		t.Errorf("Addr() before Start() = %q, want empty", addr)
	}
}

func TestHandleClusterDevices_NoCluster(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["total"].(float64) != 3 {
		t.Errorf("total = %v, want 3", body["total"])
	}
}

func TestHandleClusterDevices_WithClusterNoRemotes(t *testing.T) {
	mock := newTestMock()
	// Members with HTTPPort=0 so no remote fetches happen
	clusterMock := &mockClusterAPI{
		self: cluster.NodeInfo{ID: "node-1", Host: "localhost"},
		members: []cluster.NodeInfo{
			{ID: "node-1", Host: "localhost", HTTPPort: 0},
		},
	}
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	srv.SetClusterAPI(clusterMock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if int(body["total"].(float64)) != 3 {
		t.Errorf("total = %v, want 3", body["total"])
	}
	if body["self"] != "node-1" {
		t.Errorf("self = %v, want node-1", body["self"])
	}
}

func TestHandleClusterStats_NoCluster(t *testing.T) {
	mock := newTestMock()
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["total_connections"].(float64) != 3 {
		t.Errorf("total_connections = %v, want 3", body["total_connections"])
	}
}

func TestHandleClusterStats_WithClusterNoRemotes(t *testing.T) {
	mock := newTestMock()
	clusterMock := &mockClusterAPI{
		self: cluster.NodeInfo{ID: "node-1", Host: "localhost"},
		members: []cluster.NodeInfo{
			{ID: "node-1", Host: "localhost", HTTPPort: 0},
		},
	}
	srv := New(":0", nil)
	srv.SetBrokerAPI(mock)
	srv.SetClusterAPI(clusterMock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["self"] != "node-1" {
		t.Errorf("self = %v, want node-1", body["self"])
	}
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 1 {
		t.Errorf("expected 1 node stats entry, got %d", len(nodes))
	}
}

func TestHandleClusterDevices_FetchRemoteDevices(t *testing.T) {
	// Start a "remote" server acting as the second node
	remoteMock := &mockBrokerAPI{
		clientIDs: []string{"remote-dev"},
		clientInfos: map[string]*broker.ClientInfo{
			"remote-dev": {
				ClientID:        "remote-dev",
				Username:        "u",
				RemoteAddr:      "1.2.3.4:1234",
				ProtocolVersion: 4,
				ConnectedAt:     time.Now(),
				KeepAlive:       60 * time.Second,
			},
		},
	}
	remoteSrv := New(":0", nil)
	remoteSrv.SetBrokerAPI(remoteMock)
	if err := remoteSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer remoteSrv.Stop()

	// Parse the remote port
	remoteAddr := remoteSrv.Addr()
	var remotePort int
	fmt.Sscanf(strings.Split(remoteAddr, ":")[len(strings.Split(remoteAddr, ":"))-1], "%d", &remotePort)

	// Local server with cluster API pointing to the remote
	localMock := newTestMock()
	clusterMock := &mockClusterAPI{
		self: cluster.NodeInfo{ID: "node-1", Host: "127.0.0.1"},
		members: []cluster.NodeInfo{
			{ID: "node-1", Host: "127.0.0.1", HTTPPort: 0}, // self, skip
			{ID: "node-2", Host: "127.0.0.1", HTTPPort: remotePort},
		},
	}
	srv := New(":0", nil)
	srv.SetBrokerAPI(localMock)
	srv.SetClusterAPI(clusterMock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/devices")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	// 3 local + 1 remote
	if int(body["total"].(float64)) != 4 {
		t.Errorf("total = %v, want 4", body["total"])
	}
}

func TestHandleClusterStats_FetchRemoteStats(t *testing.T) {
	// Start a "remote" server acting as the second node
	remoteMock := &mockBrokerAPI{clientIDs: []string{"remote-dev-1", "remote-dev-2"}}
	remoteSrv := New(":0", nil)
	remoteSrv.SetBrokerAPI(remoteMock)
	if err := remoteSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer remoteSrv.Stop()

	// Parse the remote port
	remoteAddr := remoteSrv.Addr()
	var remotePort int
	fmt.Sscanf(strings.Split(remoteAddr, ":")[len(strings.Split(remoteAddr, ":"))-1], "%d", &remotePort)

	localMock := newTestMock() // 3 clients
	clusterMock := &mockClusterAPI{
		self: cluster.NodeInfo{ID: "node-1", Host: "127.0.0.1"},
		members: []cluster.NodeInfo{
			{ID: "node-1", Host: "127.0.0.1", HTTPPort: 0},
			{ID: "node-2", Host: "127.0.0.1", HTTPPort: remotePort},
		},
	}
	srv := New(":0", nil)
	srv.SetBrokerAPI(localMock)
	srv.SetClusterAPI(clusterMock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/cluster/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	// 3 local + 2 remote = 5
	if int(body["total_connections"].(float64)) != 5 {
		t.Errorf("total_connections = %v, want 5", body["total_connections"])
	}
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 2 {
		t.Errorf("nodes = %d, want 2", len(nodes))
	}
}