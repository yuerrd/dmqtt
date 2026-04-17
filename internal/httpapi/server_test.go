package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

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