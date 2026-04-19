package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/langzp/dmqtt/internal/broker"
	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessChecker reports whether the service is ready to accept traffic.
type ReadinessChecker interface {
	IsReady() bool
}

// BrokerAPI provides device and stats data.
type BrokerAPI interface {
	ConnectedClientIDs() []string
	GetClientInfo(clientID string) *broker.ClientInfo
	GetSessionData(clientID string) *cluster.MigrateSessionData
	ClientCount() int
	ActiveSubscriptions() int
	RetainedMessageCount() int
	ClusterNodeCount() int
	DisconnectDevice(deviceID string)
}

// ClusterAPI provides cluster node information.
type ClusterAPI interface {
	Self() cluster.NodeInfo
	Members() []cluster.NodeInfo
}

// Server serves HTTP endpoints for observability.
type Server struct {
	httpServer  *http.Server
	checker     ReadinessChecker
	listener    net.Listener
	coordinator *cluster.MigrationCoordinator
	brokerAPI   BrokerAPI
	clusterAPI  ClusterAPI
	startTime   time.Time
}

// New creates a new HTTP API server.
func New(addr string, checker ReadinessChecker) *Server {
	s := &Server{checker: checker, startTime: time.Now()}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/api/v1/cluster/devices", s.handleClusterDevices)
	mux.HandleFunc("/api/v1/cluster/stats", s.handleClusterStats)
	mux.HandleFunc("/api/v1/cluster/migrations", s.handleMigrations)
	mux.HandleFunc("/api/v1/cluster/migrations/", s.handleMigrationByID)
	mux.HandleFunc("/api/v1/devices", s.handleDevices)
	mux.HandleFunc("/api/v1/devices/", s.handleDeviceByID)
	mux.HandleFunc("/api/v1/stats", s.handleStats)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.Handle("/admin/", adminHandler())

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

// Start begins listening and serving HTTP requests.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.listener = ln

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()
	return nil
}

// Addr returns the listener address, useful when using port 0 in tests.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.checker != nil && s.checker.IsReady() {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
	}
}

// SetMigrationCoordinator sets the migration coordinator for API endpoints.
func (s *Server) SetMigrationCoordinator(coord *cluster.MigrationCoordinator) {
	s.coordinator = coord
}

// SetBrokerAPI sets the broker API for device management endpoints.
func (s *Server) SetBrokerAPI(api BrokerAPI) {
	s.brokerAPI = api
}

// SetClusterAPI sets the cluster API for node information endpoints.
func (s *Server) SetClusterAPI(api ClusterAPI) {
	s.clusterAPI = api
}

func (s *Server) handleMigrations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.coordinator == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "migrations not configured"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		migrations := s.coordinator.List()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(migrations)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleMigrationByID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.coordinator == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "migrations not configured"})
		return
	}

	// Parse ID from path: /api/v1/cluster/migrations/{id}[/cancel]
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/cluster/migrations/")
	parts := strings.Split(path, "/")
	id := parts[0]

	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		if s.coordinator.Cancel(id) {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
		} else {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "migration not found"})
		}
		return
	}

	if r.Method == http.MethodGet {
		m := s.coordinator.Get(id)
		if m == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "migration not found"})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(m)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.brokerAPI == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "broker API not configured"})
		return
	}
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	nodeID := ""
	if s.clusterAPI != nil {
		nodeID = s.clusterAPI.Self().ID
	}
	stats := map[string]interface{}{
		"node_id":              nodeID,
		"connected_clients":    s.brokerAPI.ClientCount(),
		"active_subscriptions": s.brokerAPI.ActiveSubscriptions(),
		"retained_messages":    s.brokerAPI.RetainedMessageCount(),
		"cluster_nodes":        s.brokerAPI.ClusterNodeCount(),
		"uptime_seconds":       int(time.Since(s.startTime).Seconds()),
		"goroutines":           runtime.NumGoroutine(),
		"memory_alloc_bytes":   memStats.Alloc,
		"memory_sys_bytes":     memStats.Sys,
		"gc_pause_total_ns":    memStats.PauseTotalNs,
	}
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type nodesResponse struct {
		Nodes []cluster.NodeInfo `json:"nodes"`
		Self  string             `json:"self"`
	}
	resp := nodesResponse{Nodes: []cluster.NodeInfo{}}
	if s.clusterAPI != nil {
		resp.Self = s.clusterAPI.Self().ID
		resp.Nodes = s.clusterAPI.Members()
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.brokerAPI == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "broker API not configured"})
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if perPage < 1 {
		perPage = 20
	}
	if perPage > 100 {
		perPage = 100
	}

	ids := s.brokerAPI.ConnectedClientIDs()
	sort.Strings(ids)
	total := len(ids)

	start := (page - 1) * perPage
	if start > total {
		start = total
	}
	end := start + perPage
	if end > total {
		end = total
	}
	pageIDs := ids[start:end]

	type deviceSummary struct {
		ClientID        string   `json:"client_id"`
		Username        string   `json:"username"`
		RemoteAddr      string   `json:"remote_addr"`
		ProtocolVersion byte     `json:"protocol_version"`
		ConnectedAt     string   `json:"connected_at"`
		KeepAlive       int      `json:"keep_alive"`
		Subscriptions   []string `json:"subscriptions"`
	}

	nodeID := ""
	if s.clusterAPI != nil {
		nodeID = s.clusterAPI.Self().ID
	}

	devices := make([]deviceSummary, 0, len(pageIDs))
	for _, id := range pageIDs {
		info := s.brokerAPI.GetClientInfo(id)
		if info == nil {
			continue
		}
		var subs []string
		if sess := s.brokerAPI.GetSessionData(id); sess != nil {
			for topic := range sess.Subscriptions {
				subs = append(subs, topic)
			}
		}
		devices = append(devices, deviceSummary{
			ClientID:        info.ClientID,
			Username:        info.Username,
			RemoteAddr:      info.RemoteAddr,
			ProtocolVersion: info.ProtocolVersion,
			ConnectedAt:     info.ConnectedAt.UTC().Format("2006-01-02T15:04:05Z"),
			KeepAlive:       int(info.KeepAlive.Seconds()),
			Subscriptions:   subs,
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"devices":  devices,
		"total":    total,
		"page":     page,
		"per_page": perPage,
		"node_id":  nodeID,
	})
}

func (s *Server) handleDeviceByID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.brokerAPI == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "broker API not configured"})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/devices/")
	parts := strings.SplitN(path, "/", 2)
	deviceID := parts[0]

	if deviceID == "" {
		s.handleDevices(w, r)
		return
	}

	if len(parts) == 2 && parts[1] == "disconnect" && r.Method == http.MethodPost {
		info := s.brokerAPI.GetClientInfo(deviceID)
		if info == nil {
			// Proxy disconnect to remote node
			if s.clusterAPI != nil {
				for _, node := range s.clusterAPI.Members() {
					if node.ID == s.clusterAPI.Self().ID || node.HTTPPort == 0 {
						continue
					}
					if s.proxyDeviceDisconnect(w, node, deviceID) {
						return
					}
				}
			}
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "device not found"})
			return
		}
		s.brokerAPI.DisconnectDevice(deviceID)
		json.NewEncoder(w).Encode(map[string]string{"status": "disconnected"})
		return
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	info := s.brokerAPI.GetClientInfo(deviceID)
	if info == nil {
		// Try proxy to remote node that owns this device
		if s.clusterAPI != nil {
			for _, node := range s.clusterAPI.Members() {
				if node.ID == s.clusterAPI.Self().ID || node.HTTPPort == 0 {
					continue
				}
				if s.proxyDeviceDetail(w, node, deviceID) {
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "device not found"})
		return
	}

	type sessionInfo struct {
		CleanStart     bool            `json:"clean_start"`
		ExpiryInterval uint32          `json:"expiry_interval"`
		Subscriptions  map[string]byte `json:"subscriptions"`
	}

	type deviceDetail struct {
		ClientID        string       `json:"client_id"`
		Username        string       `json:"username"`
		RemoteAddr      string       `json:"remote_addr"`
		ProtocolVersion byte         `json:"protocol_version"`
		ConnectedAt     string       `json:"connected_at"`
		KeepAlive       int          `json:"keep_alive"`
		Connected       bool         `json:"connected"`
		Session         *sessionInfo `json:"session,omitempty"`
	}

	detail := deviceDetail{
		ClientID:        info.ClientID,
		Username:        info.Username,
		RemoteAddr:      info.RemoteAddr,
		ProtocolVersion: info.ProtocolVersion,
		ConnectedAt:     info.ConnectedAt.UTC().Format("2006-01-02T15:04:05Z"),
		KeepAlive:       int(info.KeepAlive.Seconds()),
		Connected:       true,
	}

	sess := s.brokerAPI.GetSessionData(deviceID)
	if sess != nil {
		detail.Session = &sessionInfo{
			CleanStart:     sess.CleanStart,
			ExpiryInterval: sess.ExpiryInterval,
			Subscriptions:  sess.Subscriptions,
		}
	}

	json.NewEncoder(w).Encode(detail)
}

// clusterDeviceItem is used for cluster-wide device aggregation.
type clusterDeviceItem struct {
	ClientID        string   `json:"client_id"`
	Username        string   `json:"username"`
	RemoteAddr      string   `json:"remote_addr"`
	ProtocolVersion byte     `json:"protocol_version"`
	ConnectedAt     string   `json:"connected_at"`
	KeepAlive       int      `json:"keep_alive"`
	Subscriptions   []string `json:"subscriptions"`
	NodeID          string   `json:"node_id"`
}

// handleClusterDevices aggregates devices from all cluster nodes via backend proxy.
func (s *Server) handleClusterDevices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	selfID := ""
	if s.clusterAPI != nil {
		selfID = s.clusterAPI.Self().ID
	}

	// Collect local devices
	localDevices := s.collectLocalDevices(selfID)

	// If no cluster, return local only
	if s.clusterAPI == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"devices": localDevices,
			"total":   len(localDevices),
			"self":    selfID,
		})
		return
	}

	allDevices := make([]clusterDeviceItem, 0, len(localDevices))
	allDevices = append(allDevices, localDevices...)

	// Fetch from remote nodes in parallel
	members := s.clusterAPI.Members()
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range members {
		if node.ID == selfID || node.HTTPPort == 0 {
			continue
		}
		wg.Add(1)
		go func(n cluster.NodeInfo) {
			defer wg.Done()
			devices := s.fetchRemoteDevices(n)
			mu.Lock()
			allDevices = append(allDevices, devices...)
			mu.Unlock()
		}(node)
	}
	wg.Wait()

	sort.Slice(allDevices, func(i, j int) bool {
		return allDevices[i].ClientID < allDevices[j].ClientID
	})

	json.NewEncoder(w).Encode(map[string]interface{}{
		"devices": allDevices,
		"total":   len(allDevices),
		"self":    selfID,
	})
}

func (s *Server) collectLocalDevices(nodeID string) []clusterDeviceItem {
	ids := s.brokerAPI.ConnectedClientIDs()
	devices := make([]clusterDeviceItem, 0, len(ids))
	for _, id := range ids {
		info := s.brokerAPI.GetClientInfo(id)
		if info == nil {
			continue
		}
		var subs []string
		if sess := s.brokerAPI.GetSessionData(id); sess != nil {
			for topic := range sess.Subscriptions {
				subs = append(subs, topic)
			}
		}
		devices = append(devices, clusterDeviceItem{
			ClientID:        info.ClientID,
			Username:        info.Username,
			RemoteAddr:      info.RemoteAddr,
			ProtocolVersion: info.ProtocolVersion,
			ConnectedAt:     info.ConnectedAt.UTC().Format("2006-01-02T15:04:05Z"),
			KeepAlive:       int(info.KeepAlive.Seconds()),
			Subscriptions:   subs,
			NodeID:          nodeID,
		})
	}
	return devices
}

func (s *Server) fetchRemoteDevices(node cluster.NodeInfo) []clusterDeviceItem {
	url := fmt.Sprintf("http://%s:%d/api/v1/devices?page=1&per_page=1000", node.Host, node.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Warn("failed to create request for remote node", "node", node.ID, "error", err)
		return nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("failed to fetch devices from remote node", "node", node.ID, "error", err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	var result struct {
		Devices []clusterDeviceItem `json:"devices"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil
	}

	for i := range result.Devices {
		result.Devices[i].NodeID = node.ID
	}
	return result.Devices
}

type nodeStats struct {
	NodeID      string `json:"node_id"`
	Connections int    `json:"connections"`
	Subs        int    `json:"subscriptions"`
	Retained    int    `json:"retained"`
}

// handleClusterStats aggregates stats from all cluster nodes.
func (s *Server) handleClusterStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	selfID := ""
	if s.clusterAPI != nil {
		selfID = s.clusterAPI.Self().ID
	}

	local := nodeStats{
		NodeID:      selfID,
		Connections: s.brokerAPI.ClientCount(),
		Subs:        s.brokerAPI.ActiveSubscriptions(),
		Retained:    s.brokerAPI.RetainedMessageCount(),
	}
	allStats := []nodeStats{local}
	totalConns := local.Connections
	totalSubs := local.Subs

	if s.clusterAPI != nil {
		members := s.clusterAPI.Members()
		var mu sync.Mutex
		var wg sync.WaitGroup

		for _, node := range members {
			if node.ID == selfID || node.HTTPPort == 0 {
				continue
			}
			wg.Add(1)
			go func(n cluster.NodeInfo) {
				defer wg.Done()
				ns := s.fetchRemoteStats(n)
				if ns != nil {
					mu.Lock()
					allStats = append(allStats, *ns)
					totalConns += ns.Connections
					totalSubs += ns.Subs
					mu.Unlock()
				}
			}(node)
		}
		wg.Wait()
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes":               allStats,
		"total_connections":   totalConns,
		"total_subscriptions": totalSubs,
		"self":                selfID,
	})
}

func (s *Server) fetchRemoteStats(node cluster.NodeInfo) *nodeStats {
	url := fmt.Sprintf("http://%s:%d/api/v1/stats", node.Host, node.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}

	conns, _ := raw["connected_clients"].(float64)
	subs, _ := raw["active_subscriptions"].(float64)
	retained, _ := raw["retained_messages"].(float64)

	return &nodeStats{
		NodeID:      node.ID,
		Connections: int(conns),
		Subs:        int(subs),
		Retained:    int(retained),
	}
}

// proxyDeviceDetail forwards a device detail request to a remote node.
// Returns true if the remote node had the device.
func (s *Server) proxyDeviceDetail(w http.ResponseWriter, node cluster.NodeInfo, deviceID string) bool {
	url := fmt.Sprintf("http://%s:%d/api/v1/devices/%s", node.Host, node.HTTPPort, deviceID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
	return true
}

// proxyDeviceDisconnect forwards a disconnect request to a remote node.
func (s *Server) proxyDeviceDisconnect(w http.ResponseWriter, node cluster.NodeInfo, deviceID string) bool {
	url := fmt.Sprintf("http://%s:%d/api/v1/devices/%s/disconnect", node.Host, node.HTTPPort, deviceID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
	return true
}


