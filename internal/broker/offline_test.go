package broker

import (
	"testing"
	"time"
)

func TestOfflineStore_EnqueueDequeue(t *testing.T) {
	store := NewOfflineStore(10, 1*time.Minute)

	clientID := "client1"
	msg := &OfflineMessage{
		Topic:   "test/topic",
		Payload: []byte("hello"),
		QoS:     1,
	}

	// Enqueue message
	if err := store.Enqueue(clientID, msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Dequeue message
	messages := store.Dequeue(clientID, 10)
	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}
	if messages[0].Topic != "test/topic" {
		t.Errorf("Topic = %s, want test/topic", messages[0].Topic)
	}

	// Dequeue again should return empty
	messages = store.Dequeue(clientID, 10)
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages after dequeue, got %d", len(messages))
	}
}

func TestOfflineStore_PriorityOrdering(t *testing.T) {
	store := NewOfflineStore(10, 1*time.Minute)

	clientID := "client1"

	// Enqueue in order: low, high, medium
	msgLow := &OfflineMessage{Topic: "low", Payload: []byte("low"), QoS: 1, Priority: PriorityLow}
	msgHigh := &OfflineMessage{Topic: "high", Payload: []byte("high"), QoS: 1, Priority: PriorityHigh}
	msgMedium := &OfflineMessage{Topic: "medium", Payload: []byte("medium"), QoS: 1, Priority: PriorityMedium}

	store.Enqueue(clientID, msgLow)
	store.Enqueue(clientID, msgHigh)
	store.Enqueue(clientID, msgMedium)

	// Dequeue should return in priority order: high, medium, low
	messages := store.Dequeue(clientID, 10)
	if len(messages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(messages))
	}

	if messages[0].Topic != "high" {
		t.Errorf("First message topic = %s, want high", messages[0].Topic)
	}
	if messages[1].Topic != "medium" {
		t.Errorf("Second message topic = %s, want medium", messages[1].Topic)
	}
	if messages[2].Topic != "low" {
		t.Errorf("Third message topic = %s, want low", messages[2].Topic)
	}
}

func TestOfflineStore_CapacityLimit(t *testing.T) {
	store := NewOfflineStore(2, 1*time.Minute)

	clientID := "client1"

	msg1 := &OfflineMessage{Topic: "t1", Payload: []byte("1"), QoS: 1}
	msg2 := &OfflineMessage{Topic: "t2", Payload: []byte("2"), QoS: 1}
	msg3 := &OfflineMessage{Topic: "t3", Payload: []byte("3"), QoS: 1}

	// First two should succeed
	if err := store.Enqueue(clientID, msg1); err != nil {
		t.Fatalf("Enqueue msg1 failed: %v", err)
	}
	if err := store.Enqueue(clientID, msg2); err != nil {
		t.Fatalf("Enqueue msg2 failed: %v", err)
	}

	// Third should fail (queue full)
	if err := store.Enqueue(clientID, msg3); err == nil {
		t.Error("Enqueue msg3 should have failed (queue full)")
	}
}

func TestOfflineStore_TTLExpiry(t *testing.T) {
	store := NewOfflineStore(10, 50*time.Millisecond)

	clientID := "client1"
	msg := &OfflineMessage{
		Topic:   "test/topic",
		Payload: []byte("hello"),
		QoS:     1,
	}

	// Enqueue message
	if err := store.Enqueue(clientID, msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Dequeue should return 0 messages (expired)
	messages := store.Dequeue(clientID, 10)
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages after expiry, got %d", len(messages))
	}
}

func TestOfflineStore_RemoveAll(t *testing.T) {
	store := NewOfflineStore(10, 1*time.Minute)

	clientID := "client1"

	msg1 := &OfflineMessage{Topic: "t1", Payload: []byte("1"), QoS: 1}
	msg2 := &OfflineMessage{Topic: "t2", Payload: []byte("2"), QoS: 1}

	// Enqueue messages
	store.Enqueue(clientID, msg1)
	store.Enqueue(clientID, msg2)

	// RemoveAll
	store.RemoveAll(clientID)

	// Dequeue should return 0 messages
	messages := store.Dequeue(clientID, 10)
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages after RemoveAll, got %d", len(messages))
	}
}
