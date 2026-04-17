package cluster

import (
	"fmt"
	"log/slog"
	"time"
)

// MigrationExecutor runs a single ShardMigration through its phases.
type MigrationExecutor struct {
	transport *PeerTransport
	connIdx   *ConnectionIndex
	broker    MigrationBrokerAPI
}

// NewMigrationExecutor creates a new executor.
func NewMigrationExecutor(transport *PeerTransport, connIdx *ConnectionIndex, broker MigrationBrokerAPI) *MigrationExecutor {
	return &MigrationExecutor{
		transport: transport,
		connIdx:   connIdx,
		broker:    broker,
	}
}

// Execute runs all phases of a migration. Returns nil on success.
func (e *MigrationExecutor) Execute(m *ShardMigration) error {
	slog.Info("migration: starting", "id", m.ID, "from", m.FromNode, "to", m.ToNode, "devices", m.Progress.Total)

	// Phase 1: Prepare — mark devices as migrating
	m.AdvancePhase() // pending → running, phase → Handshake (we start from Prepare)
	for _, deviceID := range m.AffectedDevices {
		e.connIdx.SetMigrating(deviceID, true)
	}
	slog.Info("migration: prepare complete", "id", m.ID)

	// Phase 2: Handshake — skip explicit handshake, rely on PeerTransport connectivity
	m.AdvancePhase()
	slog.Info("migration: handshake complete", "id", m.ID)

	// Phase 3: Migrate — batch disconnect + data transfer
	m.AdvancePhase()
	devices := m.AffectedDevices
	batchSize := m.Config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(devices); i += batchSize {
		end := i + batchSize
		if end > len(devices) {
			end = len(devices)
		}
		batch := devices[i:end]

		batchMigrated := 0
		batchFailed := 0

		for _, deviceID := range batch {
			// Disconnect the device
			e.broker.DisconnectDevice(deviceID)

			// Gather data to migrate
			offlineMsgs := e.broker.GetOfflineMessages(deviceID)
			sessionData := e.broker.GetSessionData(deviceID)

			migrateMsg := MigrateDataMessage{
				DeviceID: deviceID,
				Messages: offlineMsgs,
				Session:  sessionData,
			}

			// Send data to target node
			var sendErr error
			for retry := 0; retry <= m.Config.MaxRetries; retry++ {
				sendErr = e.transport.SendMigrate(m.ToNode, migrateMsg)
				if sendErr == nil {
					break
				}
				slog.Warn("migration: send failed, retrying", "device", deviceID, "retry", retry, "error", sendErr)
			}

			if sendErr != nil {
				slog.Error("migration: device migration failed", "device", deviceID, "error", sendErr)
				batchFailed++
				continue
			}

			// Cleanup source data
			e.broker.DeleteOfflineMessages(deviceID)
			e.broker.DeleteSession(deviceID)
			batchMigrated++
		}

		m.RecordMigrated(batchMigrated)
		m.RecordFailed(batchFailed)

		// Sleep between batches (except last)
		if end < len(devices) && m.Config.BatchInterval > 0 {
			time.Sleep(m.Config.BatchInterval)
		}
	}

	slog.Info("migration: migrate phase complete", "id", m.ID, "migrated", m.Progress.Migrated, "failed", m.Progress.Failed)

	// Phase 4: Redirect — clear migrating flags
	m.AdvancePhase()
	for _, deviceID := range m.AffectedDevices {
		e.connIdx.SetMigrating(deviceID, false)
		e.connIdx.Remove(deviceID)
	}
	slog.Info("migration: redirect complete", "id", m.ID)

	// Phase 5: Cleanup — already done per-device above
	m.AdvancePhase()

	if m.Progress.Failed > 0 {
		m.MarkFailed(fmt.Sprintf("%d devices failed to migrate", m.Progress.Failed))
		return fmt.Errorf("migration %s: %d devices failed", m.ID, m.Progress.Failed)
	}

	m.MarkCompleted()
	slog.Info("migration: completed", "id", m.ID)
	return nil
}
