package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/langzp/dmqtt/internal/broker"
	"github.com/langzp/dmqtt/internal/cluster"
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