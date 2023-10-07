package ipfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.uber.org/zap"
)

type (
	// A Store is a persistent store for IPFS blocks
	Store interface {
		GetBlock(ctx context.Context, c cid.Cid) (Block, error)
		HasBlock(ctx context.Context, c cid.Cid) (bool, error)
		AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
	}

	// A RenterdBlockStore is a blockstore backed by a renterd node IPFS blocks
	// are stored in a local database and backed by a renterd node. The primary
	// difference between this and a normal IPFS blockstore is that an object is
	// stored on the renterd node in one piece and the offsets for each block
	// are stored in the database.
	RenterdBlockStore struct {
		store   Store
		log     *zap.Logger
		renterd config.Renterd
	}
)

// DeleteBlock removes a given block from the blockstore.
// note: this is not implemented
func (bs *RenterdBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("cannot put blocks")
}

// Has returns whether or not a given block is in the blockstore.
func (bs *RenterdBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	bs.log.Debug("has block", zap.String("cid", c.Hash().B58String()))
	return bs.store.HasBlock(ctx, c)
}

// Get returns a block by CID
func (bs *RenterdBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	bs.log.Debug("get block", zap.String("cid", c.Hash().B58String()))
	cm, err := bs.store.GetBlock(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get cid: %w", err)
	}

	var buf bytes.Buffer
	r, err := downloadObject(ctx, bs.renterd, cm.Key, cm.Offset, cm.Length)
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
func (bs *RenterdBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	bs.log.Debug("get size", zap.String("cid", c.Hash().B58String()))
	cm, err := bs.store.GetBlock(ctx, c)
	return int(cm.Length), err
}

// Put puts a given block to the underlying datastore
func (bs *RenterdBlockStore) Put(context.Context, blocks.Block) error {
	return errors.New("cannot put blocks")
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *RenterdBlockStore) PutMany(context.Context, []blocks.Block) error {
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
func (bs *RenterdBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.store.AllKeysChan(ctx)
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *RenterdBlockStore) HashOnRead(enabled bool) {
	// TODO: implement
}

// NewRenterdBlockStore creates a new blockstore backed by the given
// badger.Store and a renterd node
func NewRenterdBlockStore(store Store, renterd config.Renterd) *RenterdBlockStore {
	return &RenterdBlockStore{
		store:   store,
		renterd: renterd,
	}
}
