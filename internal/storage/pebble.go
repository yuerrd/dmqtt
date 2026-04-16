package storage

import (
	"bytes"

	"github.com/cockroachdb/pebble"
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
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (s *PebbleStore) Set(key []byte, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

func (s *PebbleStore) Delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

func (s *PebbleStore) Scan(prefix []byte, fn func(key, value []byte) error) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		keyCopy := make([]byte, len(iter.Key()))
		copy(keyCopy, iter.Key())
		valCopy := make([]byte, len(iter.Value()))
		copy(valCopy, iter.Value())
		if err := fn(keyCopy, valCopy); err != nil {
			return err
		}
	}
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
