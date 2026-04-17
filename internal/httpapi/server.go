package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/langzp/dmqtt/internal/cluster"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessChecker reports whether the service is ready to accept traffic.
type ReadinessChecker interface {
	IsReady() bool
}

// Server serves HTTP endpoints for observability.
type Server struct {
	httpServer  *http.Server
	checker     ReadinessChecker
	listener    net.Listener
	coordinator *cluster.MigrationCoordinator
}

// New creates a new HTTP API server.
func New(addr string, checker ReadinessChecker) *Server {
	s := &Server{checker: checker}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/api/v1/cluster/migrations", s.handleMigrations)
	mux.HandleFunc("/api/v1/cluster/migrations/", s.handleMigrationByID)

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