package badger

import (
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// A Store is a badger-backed store.
type Store struct {
	db  *badger.DB
	log *zap.Logger
}

// Close closes the underlying database
func (s *Store) Close() error {
	return s.db.Close()
}

// OpenDatabase opens a badger database at the given path.
func OpenDatabase(path string, log *zap.Logger) (*Store, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &Store{db: db, log: log}, nil
}
