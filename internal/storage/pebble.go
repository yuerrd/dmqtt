package storage

import (
	"bytes"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/langzp/dmqtt/internal/metrics"
)

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(dir string) (*PebbleStore, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db}, nil
}

func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	start := time.Now()
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		metrics.StorageReadLatency("get", time.Since(start))
		return nil, nil
	}
	if err != nil {
		metrics.StorageReadLatency("get", time.Since(start))
		return nil, err
	}
	defer closer.Close()
	result := make([]byte, len(val))
	copy(result, val)
	metrics.StorageReadLatency("get", time.Since(start))
	return result, nil
}

func (s *PebbleStore) Set(key []byte, value []byte) error {
	start := time.Now()
	err := s.db.Set(key, value, pebble.Sync)
	metrics.StorageWriteLatency("set", time.Since(start))
	return err
}

func (s *PebbleStore) Delete(key []byte) error {
	start := time.Now()
	err := s.db.Delete(key, pebble.Sync)
	metrics.StorageWriteLatency("delete", time.Since(start))
	return err
}

func (s *PebbleStore) Scan(prefix []byte, fn func(key, value []byte) error) error {
	start := time.Now()
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		metrics.StorageReadLatency("scan", time.Since(start))
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		keyCopy := make([]byte, len(iter.Key()))
		copy(keyCopy, iter.Key())
		valCopy := make([]byte, len(iter.Value()))
		copy(valCopy, iter.Value())
		if err := fn(keyCopy, valCopy); err != nil {
			metrics.StorageReadLatency("scan", time.Since(start))
			return err
		}
	}
	metrics.StorageReadLatency("scan", time.Since(start))
	return iter.Error()
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < 0xFF {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
