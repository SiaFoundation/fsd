package blockstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/persist/badger"
)

// A Store implements the IPFS blockstore interface
type Store struct {
	bucket string

	db      *badger.Store
	renterd config.Renterd
}

// DeleteBlock removes a given block from the blockstore.
// note: this is not implemented
func (s *Store) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("cannot put blocks")
}

// Has returns whether or not a given block is in the blockstore.
func (s *Store) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return s.db.HasBlock(c)
}

// Get returns a block by CID
func (s *Store) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	cm, err := s.db.GetBlock(c)
	if err != nil {
		return nil, fmt.Errorf("failed to get cid: %w", err)
	}

	var buf bytes.Buffer
	r, err := downloadObject(ctx, s.renterd, cm.Key, cm.Offset, cm.Length)
	if err != nil {
		return nil, fmt.Errorf("failed to download object: %w", err)
	}
	defer r.Close()

	if n, err := io.Copy(&buf, r); err != nil {
		return nil, fmt.Errorf("failed to copy object: %w", err)
	} else if n != int64(cm.Length) {
		return nil, fmt.Errorf("failed to copy object: expected %d bytes, got %d", cm.Length, n)
	}

	return blocks.NewBlockWithCid(buf.Bytes(), c)
}

// GetSize returns the CIDs mapped BlockSize
func (s *Store) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	cm, err := s.db.GetBlock(c)
	return int(cm.Length), err
}

// Put puts a given block to the underlying datastore
func (s *Store) Put(context.Context, blocks.Block) error {
	return errors.New("cannot put blocks")
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (s *Store) PutMany(context.Context, []blocks.Block) error {
	return errors.New("cannot put blocks")
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
//
// AllKeysChan treats the underlying blockstore as a set, and returns that
// set in full. The only guarantee is that the consumer of AKC will
// encounter every CID in the underlying set, at least once. If the
// underlying blockstore supports duplicate CIDs it is up to the
// implementation to elect to return such duplicates or not. Similarly no
// guarantees are made regarding CID ordering.
//
// When underlying blockstore is operating on Multihash and codec information
// is not preserved, returned CIDs will use Raw (0x55) codec.
func (s *Store) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return s.db.AllKeysChan(ctx)
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (s *Store) HashOnRead(enabled bool) {
	// TODO: implement
}

// unlocker is a stub implementation of blockstore.Unlocker
type unlocker struct{}

func (u unlocker) Unlock(context.Context) {}

// GCLock locks the blockstore for garbage collection. No operations
// that expect to finish with a pin should occur simultaneously.
// Reading during GC is safe, and requires no lock.
func (s *Store) GCLock(context.Context) blockstore.Unlocker {
	return unlocker{}
}

// PinLock locks the blockstore for sequences of puts expected to finish
// with a pin (before GC). Multiple put->pin sequences can write through
// at the same time, but no GC should happen simultaneously.
// Reading during Pinning is safe, and requires no lock.
func (s *Store) PinLock(context.Context) blockstore.Unlocker {
	return unlocker{}
}

// GCRequested returns true if GCLock has been called and is waiting to
// take the lock
func (s *Store) GCRequested(context.Context) bool {
	return false
}

// New creates a new blockstore backed by the given badger.Store and renterd client
func New(bucket string, db *badger.Store, renterd config.Renterd) *Store {
	return &Store{
		bucket:  bucket,
		db:      db,
		renterd: renterd,
	}
}
