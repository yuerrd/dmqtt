package cluster

import (
	"testing"
	"time"
)

func TestShardMigration_Transitions(t *testing.T) {
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1", "dev-2"}, DefaultMigrationConfig())

	if m.Status != MigrationPending {
		t.Fatalf("expected pending, got %v", m.Status)
	}
	if m.Phase != PhasePrepare {
		t.Fatalf("expected PhasePrepare, got %v", m.Phase)
	}
	if m.Progress.Total != 2 {
		t.Fatalf("expected 2 total, got %d", m.Progress.Total)
	}

	// Advance to handshake
	m.AdvancePhase()
	if m.Phase != PhaseHandshake {
		t.Fatalf("expected PhaseHandshake, got %v", m.Phase)
	}
	if m.Status != MigrationRunning {
		t.Fatalf("expected running, got %v", m.Status)
	}

	// Advance through remaining phases
	m.AdvancePhase() // Migrate
	if m.Phase != PhaseMigrate {
		t.Fatalf("expected PhaseMigrate, got %v", m.Phase)
	}

	m.AdvancePhase() // Redirect
	if m.Phase != PhaseRedirect {
		t.Fatalf("expected PhaseRedirect, got %v", m.Phase)
	}

	m.AdvancePhase() // Cleanup
	if m.Phase != PhaseCleanup {
		t.Fatalf("expected PhaseCleanup, got %v", m.Phase)
	}
}

func TestShardMigration_MarkCompleted(t *testing.T) {
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m.MarkCompleted()
	if m.Status != MigrationCompleted {
		t.Fatalf("expected completed, got %v", m.Status)
	}
}

func TestShardMigration_MarkFailed(t *testing.T) {
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m.MarkFailed("connection refused")
	if m.Status != MigrationFailed {
		t.Fatalf("expected failed, got %v", m.Status)
	}
	if m.Error != "connection refused" {
		t.Fatalf("expected error message, got %q", m.Error)
	}
}

func TestShardMigration_Cancel(t *testing.T) {
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m.Cancel()
	if m.Status != MigrationCancelled {
		t.Fatalf("expected cancelled, got %v", m.Status)
	}
}

func TestShardMigration_RecordProgress(t *testing.T) {
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1", "dev-2", "dev-3"}, DefaultMigrationConfig())
	m.RecordMigrated(1)
	m.RecordMigrated(1)
	m.RecordFailed(1)

	if m.Progress.Migrated != 2 {
		t.Fatalf("expected 2 migrated, got %d", m.Progress.Migrated)
	}
	if m.Progress.Failed != 1 {
		t.Fatalf("expected 1 failed, got %d", m.Progress.Failed)
	}
}

func TestDefaultMigrationConfig(t *testing.T) {
	cfg := DefaultMigrationConfig()
	if cfg.BatchSize != 1000 {
		t.Fatalf("expected batch size 1000, got %d", cfg.BatchSize)
	}
	if cfg.BatchInterval != time.Second {
		t.Fatalf("expected 1s interval, got %v", cfg.BatchInterval)
	}
	if cfg.MaxRetries != 3 {
		t.Fatalf("expected 3 retries, got %d", cfg.MaxRetries)
	}
}
