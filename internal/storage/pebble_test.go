package storage

import (
	"os"
	"testing"
)

func TestPebbleStore_GetSetDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	defer store.Close()

	// Test Get non-existent key
	val, err := store.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected nil for non-existent key, got %v", val)
	}

	// Test Set and Get
	key := []byte("testkey")
	value := []byte("testvalue")
	if err := store.Set(key, value); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != string(value) {
		t.Fatalf("Expected %s, got %s", value, val)
	}

	// Test overwrite
	newValue := []byte("newvalue")
	if err := store.Set(key, newValue); err != nil {
		t.Fatalf("Set (overwrite) failed: %v", err)
	}

	val, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	if string(val) != string(newValue) {
		t.Fatalf("Expected %s, got %s", newValue, val)
	}

	// Test Delete
	if err := store.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	val, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected nil after delete, got %v", val)
	}
}

func TestPebbleStore_Scan(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	defer store.Close()

	// Insert keys with different prefixes
	testData := map[string]string{
		"s/sub1": "value1",
		"s/sub2": "value2",
		"r/ret1": "value3",
		"o/off1": "value4",
	}

	for k, v := range testData {
		if err := store.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Scan "s/" prefix - should get 2 results
	var sResults []string
	err = store.Scan([]byte("s/"), func(key, value []byte) error {
		sResults = append(sResults, string(key))
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(sResults) != 2 {
		t.Fatalf("Expected 2 results for s/ prefix, got %d", len(sResults))
	}

	// Scan "r/" prefix - should get 1 result
	var rResults []string
	err = store.Scan([]byte("r/"), func(key, value []byte) error {
		rResults = append(rResults, string(key))
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rResults) != 1 {
		t.Fatalf("Expected 1 result for r/ prefix, got %d", len(rResults))
	}
}

func TestPebbleStore_ScanEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	defer store.Close()

	// Scan non-existent prefix
	var results []string
	err = store.Scan([]byte("nonexistent/"), func(key, value []byte) error {
		results = append(results, string(key))
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("Expected 0 results for non-existent prefix, got %d", len(results))
	}
}
