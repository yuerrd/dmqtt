package cluster

// MigrationBrokerAPI is the interface the migration system uses to interact with the broker.
// Defined in the cluster package to avoid circular dependencies.
type MigrationBrokerAPI interface {
	// DisconnectDevice forcefully disconnects a device.
	DisconnectDevice(deviceID string)

	// GetOfflineMessages returns all offline messages for a device.
	GetOfflineMessages(deviceID string) []MigrateOfflineMsg

	// DeleteOfflineMessages removes all offline messages for a device.
	DeleteOfflineMessages(deviceID string)

	// GetSessionData returns session data for migration, or nil if not found.
	GetSessionData(clientID string) *MigrateSessionData

	// DeleteSession removes a session.
	DeleteSession(clientID string)

	// ImportMigrateData stores incoming migration data (offline messages + session).
	ImportMigrateData(msg MigrateDataMessage)
}
