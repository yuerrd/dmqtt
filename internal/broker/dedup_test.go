package broker

import (
	"testing"
	"time"
)

func TestDedupStore_BasicFlow(t *testing.T) {
	store := NewDedupStore(1 * time.Minute)

	clientID := "client1"
	packetID := uint16(42)

	// Initially should not be duplicate
	if store.IsDuplicate(clientID, packetID) {
		t.Error("IsDuplicate returned true for new packet")
	}

	// Mark as received
	store.MarkReceived(clientID, packetID)

	// Now should be duplicate
	if !store.IsDuplicate(clientID, packetID) {
		t.Error("IsDuplicate returned false after MarkReceived")
	}

	// Remove it
	store.Remove(clientID, packetID)

	// Should no longer be duplicate
	if store.IsDuplicate(clientID, packetID) {
		t.Error("IsDuplicate returned true after Remove")
	}
}

func TestDedupStore_DifferentClients(t *testing.T) {
	store := NewDedupStore(1 * time.Minute)

	packetID := uint16(100)

	// Mark for client1
	store.MarkReceived("client1", packetID)

	// client1 should see duplicate
	if !store.IsDuplicate("client1", packetID) {
		t.Error("client1 should see duplicate")
	}

	// client2 should NOT see duplicate (different client)
	if store.IsDuplicate("client2", packetID) {
		t.Error("client2 should not see duplicate")
	}
}

func TestDedupStore_Expiry(t *testing.T) {
	store := NewDedupStore(50 * time.Millisecond)

	clientID := "client1"
	packetID := uint16(42)

	// Mark as received
	store.MarkReceived(clientID, packetID)

	// Should be duplicate immediately
	if !store.IsDuplicate(clientID, packetID) {
		t.Error("IsDuplicate returned false after MarkReceived")
	}

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Should no longer be duplicate (expired)
	if store.IsDuplicate(clientID, packetID) {
		t.Error("IsDuplicate returned true after expiry")
	}
}
