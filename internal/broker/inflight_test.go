package broker

import (
	"testing"
	"time"
)

func TestPacketIDAllocator_Sequential(t *testing.T) {
	alloc := NewPacketIDAllocator()

	id1 := alloc.Next()
	if id1 != 1 {
		t.Errorf("First ID = %d, want 1", id1)
	}

	id2 := alloc.Next()
	if id2 != 2 {
		t.Errorf("Second ID = %d, want 2", id2)
	}

	id3 := alloc.Next()
	if id3 != 3 {
		t.Errorf("Third ID = %d, want 3", id3)
	}
}

func TestPacketIDAllocator_WrapAround(t *testing.T) {
	alloc := NewPacketIDAllocator()
	alloc.counter = 65534

	id1 := alloc.Next()
	if id1 != 65535 {
		t.Errorf("ID before wrap = %d, want 65535", id1)
	}

	id2 := alloc.Next()
	if id2 != 1 {
		t.Errorf("ID after wrap = %d, want 1", id2)
	}
}

func TestInflightStore_BasicFlow(t *testing.T) {
	store := NewInflightStore(10)

	msg := &InflightMessage{
		PacketID:  1,
		Topic:     "test/topic",
		Payload:   []byte("hello"),
		QoS:       1,
		Timestamp: time.Now(),
	}

	// Add message
	if !store.Add(msg) {
		t.Fatal("Add failed")
	}

	// Get message
	retrieved := store.Get(1)
	if retrieved == nil {
		t.Fatal("Get returned nil")
	}
	if retrieved.Topic != "test/topic" {
		t.Errorf("Topic = %s, want test/topic", retrieved.Topic)
	}

	// Remove message
	store.Remove(1)

	// Get after remove should return nil
	retrieved = store.Get(1)
	if retrieved != nil {
		t.Error("Get after Remove should return nil")
	}
}

func TestInflightStore_Limit(t *testing.T) {
	store := NewInflightStore(2)

	msg1 := &InflightMessage{PacketID: 1, Topic: "t1", QoS: 1, Timestamp: time.Now()}
	msg2 := &InflightMessage{PacketID: 2, Topic: "t2", QoS: 1, Timestamp: time.Now()}
	msg3 := &InflightMessage{PacketID: 3, Topic: "t3", QoS: 1, Timestamp: time.Now()}

	// Add first two should succeed
	if !store.Add(msg1) {
		t.Fatal("Add msg1 failed")
	}
	if !store.Add(msg2) {
		t.Fatal("Add msg2 failed")
	}

	// Third should fail (limit reached)
	if store.Add(msg3) {
		t.Error("Add msg3 should have failed (limit reached)")
	}

	// Remove one
	store.Remove(1)

	// Now adding should succeed
	if !store.Add(msg3) {
		t.Error("Add msg3 should succeed after removing msg1")
	}
}

func TestInflightStore_Count(t *testing.T) {
	store := NewInflightStore(10)

	if store.Count() != 0 {
		t.Errorf("Initial count = %d, want 0", store.Count())
	}

	msg1 := &InflightMessage{PacketID: 1, Topic: "t1", QoS: 1, Timestamp: time.Now()}
	msg2 := &InflightMessage{PacketID: 2, Topic: "t2", QoS: 1, Timestamp: time.Now()}

	store.Add(msg1)
	if store.Count() != 1 {
		t.Errorf("Count after 1 add = %d, want 1", store.Count())
	}

	store.Add(msg2)
	if store.Count() != 2 {
		t.Errorf("Count after 2 adds = %d, want 2", store.Count())
	}

	store.Remove(1)
	if store.Count() != 1 {
		t.Errorf("Count after 1 remove = %d, want 1", store.Count())
	}
}
