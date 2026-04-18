package broker

import (
	"os"
	"testing"

	"github.com/langzp/dmqtt/internal/storage"
)

func TestWillStore_PersistAndLoad(t *testing.T) {
	dir, err := os.MkdirTemp("", "will-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pebbleStore.Close()

	ws := NewWillStore(pebbleStore)

	will := &WillMessage{
		Topic:   "will/topic",
		Payload: []byte("offline"),
		QoS:     1,
		Retain:  true,
	}
	ws.Set("client-1", will)

	got := ws.Get("client-1")
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if got.Topic != "will/topic" {
		t.Errorf("Topic = %s, want will/topic", got.Topic)
	}
	if got.QoS != 1 {
		t.Errorf("QoS = %d, want 1", got.QoS)
	}

	ws2 := NewWillStore(pebbleStore)
	if err := ws2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got2 := ws2.Get("client-1")
	if got2 == nil {
		t.Fatal("After Load, Get returned nil")
	}
	if got2.Topic != "will/topic" {
		t.Errorf("After Load: Topic = %s, want will/topic", got2.Topic)
	}
}

func TestWillStore_Delete(t *testing.T) {
	dir, err := os.MkdirTemp("", "will-delete-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pebbleStore.Close()

	ws := NewWillStore(pebbleStore)
	ws.Set("c1", &WillMessage{Topic: "t", Payload: []byte("p"), QoS: 0})
	ws.Delete("c1")

	if got := ws.Get("c1"); got != nil {
		t.Error("After Delete, Get returned non-nil")
	}

	ws2 := NewWillStore(pebbleStore)
	ws2.Load()
	if got := ws2.Get("c1"); got != nil {
		t.Error("After Delete+Load, Get returned non-nil")
	}
}

func TestWillStore_AllForNode(t *testing.T) {
	dir, err := os.MkdirTemp("", "will-allnode-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pebbleStore.Close()

	ws := NewWillStore(pebbleStore)
	ws.Set("c1", &WillMessage{Topic: "t1", Payload: []byte("p1"), QoS: 0})
	ws.Set("c2", &WillMessage{Topic: "t2", Payload: []byte("p2"), QoS: 1})
	ws.Set("c3", &WillMessage{Topic: "t3", Payload: []byte("p3"), QoS: 0})

	all := ws.All()
	if len(all) != 3 {
		t.Fatalf("All() returned %d, want 3", len(all))
	}
}

func TestWillStore_MarkPublished(t *testing.T) {
	dir, err := os.MkdirTemp("", "will-published-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pebbleStore, err := storage.NewPebbleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pebbleStore.Close()

	ws := NewWillStore(pebbleStore)

	if ws.IsPublished("c1") {
		t.Error("IsPublished should be false initially")
	}

	ws.MarkPublished("c1")

	if !ws.IsPublished("c1") {
		t.Error("IsPublished should be true after MarkPublished")
	}
}
