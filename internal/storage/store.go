package storage

// Store is the persistent key-value storage interface.
type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Scan(prefix []byte, fn func(key, value []byte) error) error
	Close() error
}
