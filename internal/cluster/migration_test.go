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

func TestMigrationCoordinator_Submit(t *testing.T) {
	coord := NewMigrationCoordinator(3)

	m1 := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m2 := NewShardMigration("mig-2", "node-1", "node-3", []string{"dev-2"}, DefaultMigrationConfig())

	coord.Submit(m1)
	coord.Submit(m2)

	if coord.Count() != 2 {
		t.Fatalf("expected 2 migrations, got %d", coord.Count())
	}

	got := coord.Get("mig-1")
	if got == nil || got.ID != "mig-1" {
		t.Fatal("Get(mig-1) failed")
	}
}

func TestMigrationCoordinator_List(t *testing.T) {
	coord := NewMigrationCoordinator(3)

	coord.Submit(NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig()))
	coord.Submit(NewShardMigration("mig-2", "node-1", "node-3", []string{"dev-2"}, DefaultMigrationConfig()))

	all := coord.List()
	if len(all) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(all))
	}
}

func TestMigrationCoordinator_Cancel(t *testing.T) {
	coord := NewMigrationCoordinator(3)
	m := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	coord.Submit(m)

	ok := coord.Cancel("mig-1")
	if !ok {
		t.Fatal("cancel should return true")
	}
	if m.Status != MigrationCancelled {
		t.Fatalf("expected cancelled, got %v", m.Status)
	}

	ok = coord.Cancel("nonexistent")
	if ok {
		t.Fatal("cancel of nonexistent should return false")
	}
}

func TestMigrationCoordinator_ActiveCount(t *testing.T) {
	coord := NewMigrationCoordinator(2)

	m1 := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m1.Status = MigrationRunning
	m2 := NewShardMigration("mig-2", "node-1", "node-3", []string{"dev-2"}, DefaultMigrationConfig())
	m2.Status = MigrationRunning
	m3 := NewShardMigration("mig-3", "node-1", "node-4", []string{"dev-3"}, DefaultMigrationConfig())

	coord.Submit(m1)
	coord.Submit(m2)
	coord.Submit(m3)

	if coord.ActiveCount() != 2 {
		t.Fatalf("expected 2 active, got %d", coord.ActiveCount())
	}

	if coord.CanStartMore() {
		t.Fatal("should not be able to start more (at max)")
	}
}

func TestMigrationCoordinator_Cleanup(t *testing.T) {
	coord := NewMigrationCoordinator(3)

	m1 := NewShardMigration("mig-1", "node-1", "node-2", []string{"dev-1"}, DefaultMigrationConfig())
	m1.MarkCompleted()
	m2 := NewShardMigration("mig-2", "node-1", "node-3", []string{"dev-2"}, DefaultMigrationConfig())

	coord.Submit(m1)
	coord.Submit(m2)

	coord.CleanupTerminal()
	if coord.Count() != 1 {
		t.Fatalf("expected 1 after cleanup, got %d", coord.Count())
	}
}
