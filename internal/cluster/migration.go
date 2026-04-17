package cluster

import (
	"sync"
	"time"
)

// MigrationPhase represents the current phase of a shard migration.
type MigrationPhase int

const (
	PhasePrepare   MigrationPhase = iota // Mark devices as migrating
	PhaseHandshake                       // Target node confirms readiness
	PhaseMigrate                         // Batch disconnect + data transfer
	PhaseRedirect                        // Gossip broadcast routing update
	PhaseCleanup                         // Source node cleans up data
)

func (p MigrationPhase) String() string {
	switch p {
	case PhasePrepare:
		return "prepare"
	case PhaseHandshake:
		return "handshake"
	case PhaseMigrate:
		return "migrate"
	case PhaseRedirect:
		return "redirect"
	case PhaseCleanup:
		return "cleanup"
	default:
		return "unknown"
	}
}

// MigrationStatus represents the overall status of a migration.
type MigrationStatus string

const (
	MigrationPending   MigrationStatus = "pending"
	MigrationRunning   MigrationStatus = "running"
	MigrationCompleted MigrationStatus = "completed"
	MigrationFailed    MigrationStatus = "failed"
	MigrationCancelled MigrationStatus = "cancelled"
)

// MigrationProgress tracks migration progress.
type MigrationProgress struct {
	Total    int `json:"total"`
	Migrated int `json:"migrated"`
	Failed   int `json:"failed"`
}

// MigrationConfig holds per-migration settings.
type MigrationConfig struct {
	BatchSize     int           `json:"batch_size"`
	BatchInterval time.Duration `json:"batch_interval"`
	MaxRetries    int           `json:"max_retries"`
}

// DefaultMigrationConfig returns sensible migration defaults.
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		BatchSize:     1000,
		BatchInterval: time.Second,
		MaxRetries:    3,
	}
}

// ShardMigration represents a single migration task.
type ShardMigration struct {
	mu              sync.Mutex
	ID              string            `json:"id"`
	FromNode        string            `json:"from_node"`
	ToNode          string            `json:"to_node"`
	AffectedDevices []string          `json:"affected_devices"`
	Phase           MigrationPhase    `json:"phase"`
	Progress        MigrationProgress `json:"progress"`
	Config          MigrationConfig   `json:"config"`
	Status          MigrationStatus   `json:"status"`
	Error           string            `json:"error,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

// NewShardMigration creates a new migration in pending state.
func NewShardMigration(id, fromNode, toNode string, devices []string, cfg MigrationConfig) *ShardMigration {
	now := time.Now()
	return &ShardMigration{
		ID:              id,
		FromNode:        fromNode,
		ToNode:          toNode,
		AffectedDevices: devices,
		Phase:           PhasePrepare,
		Progress:        MigrationProgress{Total: len(devices)},
		Config:          cfg,
		Status:          MigrationPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

// AdvancePhase moves to the next phase. Sets status to running on first advance.
func (m *ShardMigration) AdvancePhase() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Status == MigrationPending {
		m.Status = MigrationRunning
	}
	if m.Phase < PhaseCleanup {
		m.Phase++
	}
	m.UpdatedAt = time.Now()
}

// MarkCompleted marks the migration as successfully completed.
func (m *ShardMigration) MarkCompleted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Status = MigrationCompleted
	m.UpdatedAt = time.Now()
}

// MarkFailed marks the migration as failed with a reason.
func (m *ShardMigration) MarkFailed(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Status = MigrationFailed
	m.Error = reason
	m.UpdatedAt = time.Now()
}

// Cancel marks the migration as cancelled.
func (m *ShardMigration) Cancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Status = MigrationCancelled
	m.UpdatedAt = time.Now()
}

// RecordMigrated increments the migrated device count.
func (m *ShardMigration) RecordMigrated(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Progress.Migrated += n
	m.UpdatedAt = time.Now()
}

// RecordFailed increments the failed device count.
func (m *ShardMigration) RecordFailed(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Progress.Failed += n
	m.UpdatedAt = time.Now()
}

// IsTerminal returns true if the migration is in a terminal state.
func (m *ShardMigration) IsTerminal() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Status == MigrationCompleted || m.Status == MigrationFailed || m.Status == MigrationCancelled
}
