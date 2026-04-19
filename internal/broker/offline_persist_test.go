package broker

import (
	"os"
	"testing"
	"time"

	"github.com/yuerrd/dmqtt/internal/storage"
)

func TestOfflineStore_PersistAndReload(t *testing.T) {
	dir, err := os.MkdirTemp("", "offline-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	store := NewOfflineStore(1000, 24*time.Hour)
	store.SetStore(pebbleStore)

	for i := 0; i < 3; i++ {
		err := store.Enqueue("client-1", &OfflineMessage{
			Topic:   "t/" + string(rune('a'+i)),
			Payload: []byte("payload"),
			QoS:     1,
		})
		if err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	if store.Count("client-1") != 3 {
		t.Fatalf("Count = %d, want 3", store.Count("client-1"))
	}

	store2 := NewOfflineStore(1000, 24*time.Hour)
	store2.SetStore(pebbleStore)
	if err := store2.LoadFromStore(); err != nil {
		t.Fatalf("LoadFromStore: %v", err)
	}

	if store2.Count("client-1") != 3 {
		t.Fatalf("After reload: Count = %d, want 3", store2.Count("client-1"))
	}

	msgs := store2.Dequeue("client-1", 10)
	if len(msgs) != 3 {
		t.Fatalf("Dequeue after reload: got %d, want 3", len(msgs))
	}
	if msgs[0].Topic != "t/a" {
		t.Errorf("First topic = %s, want t/a", msgs[0].Topic)
	}

	store3 := NewOfflineStore(1000, 24*time.Hour)
	store3.SetStore(pebbleStore)
	if err := store3.LoadFromStore(); err != nil {
		t.Fatalf("LoadFromStore after dequeue: %v", err)
	}
	if store3.Count("client-1") != 0 {
		t.Errorf("After dequeue reload: Count = %d, want 0", store3.Count("client-1"))
	}

	pebbleStore.Close()
}

func TestOfflineStore_RemoveAllClearsPebble(t *testing.T) {
	dir, err := os.MkdirTemp("", "offline-remove-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	store := NewOfflineStore(1000, 24*time.Hour)
	store.SetStore(pebbleStore)

	store.Enqueue("client-x", &OfflineMessage{Topic: "t/1", Payload: []byte("p"), QoS: 0})
	store.Enqueue("client-x", &OfflineMessage{Topic: "t/2", Payload: []byte("p"), QoS: 0})

	store.RemoveAll("client-x")

	store2 := NewOfflineStore(1000, 24*time.Hour)
	store2.SetStore(pebbleStore)
	store2.LoadFromStore()
	if store2.Count("client-x") != 0 {
		t.Errorf("After RemoveAll reload: Count = %d, want 0", store2.Count("client-x"))
	}

	pebbleStore.Close()
}

func TestOfflineStore_NilStoreNoError(t *testing.T) {
	store := NewOfflineStore(10, time.Minute)
	if err := store.Enqueue("c1", &OfflineMessage{Topic: "t", Payload: []byte("p"), QoS: 0}); err != nil {
		t.Fatalf("Enqueue without store: %v", err)
	}
	msgs := store.Dequeue("c1", 10)
	if len(msgs) != 1 {
		t.Fatalf("Dequeue without store: got %d, want 1", len(msgs))
	}
}
