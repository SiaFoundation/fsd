package sia

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/ipld/merkledag"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.sia.tech/fsd/config"
	"go.uber.org/zap"
)

type (
	// A RenterdBlockStore is a blockstore backed by a renterd node. IPFS blocks
	// are stored in a local database and uploaded to a renterd node. The
	// primary difference between this and a normal IPFS blockstore is that a
	// file is stored on the renterd node in one piece and the offsets for
	// rebuilding the block are stored in the database.
	RenterdBlockStore struct {
		store   Store
		log     *zap.Logger
		renterd config.Renterd
	}
)

// DeleteBlock removes a given block from the blockstore.
// note: this is not implemented
func (bs *RenterdBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return nil
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
	if errors.Is(err, ErrNotFound) {
		return nil, format.ErrNotFound{Cid: c}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get cid: %w", err)
	}

	errCh := make(chan error, 2)
	defer close(errCh)

	var metaBuf, dataBuf bytes.Buffer

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		if cm.Data.BlockSize == 0 {
			return
		}

		r, err := downloadObject(ctx, bs.renterd, cm.Data.Key, cm.Data.Offset, cm.Data.BlockSize)
		if err != nil {
			errCh <- fmt.Errorf("failed to download object: %w", err)
			return
		}
		defer r.Close()

		if _, err := io.Copy(&dataBuf, r); err != nil {
			errCh <- fmt.Errorf("failed to copy object: %w", err)
			return
		}
	}()

	go func() {
		defer wg.Done()

		if cm.Metadata.Length == 0 {
			return
		}

		r, err := downloadObject(ctx, bs.renterd, cm.Metadata.Key, cm.Metadata.Offset, cm.Metadata.Length)
		if err != nil {
			errCh <- fmt.Errorf("failed to download object: %w", err)
			return
		}
		defer r.Close()

		if _, err := io.Copy(&metaBuf, r); err != nil {
			errCh <- fmt.Errorf("failed to copy object: %w", err)
			return
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	var meta pb.Data
	if err := proto.Unmarshal(metaBuf.Bytes(), &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	meta.Data = dataBuf.Bytes()

	buf, err := proto.Marshal(&meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	node := merkledag.NodeWithData(buf)
	if actual := node.Cid(); !actual.Equals(c) {
		panic(fmt.Errorf("unexpected cid: requested %q got %q", c.Hash().B58String(), actual.Hash().B58String())) // developer error
	}
	return node, nil
}

// GetSize returns the CIDs mapped BlockSize
func (bs *RenterdBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	cm, err := bs.store.GetBlock(ctx, c)
	if errors.Is(err, ErrNotFound) {
		return 0, format.ErrNotFound{Cid: c}
	} else if err != nil {
		return 0, fmt.Errorf("failed to get cid: %w", err)
	}
	bs.log.Debug("get block size", zap.String("cid", c.Hash().B58String()), zap.Uint64("size", cm.Data.BlockSize))
	return int(cm.Data.BlockSize + cm.Metadata.Length), nil
}

// Put puts a given block to the underlying datastore
func (bs *RenterdBlockStore) Put(context.Context, blocks.Block) error {
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *RenterdBlockStore) PutMany(context.Context, []blocks.Block) error {
	return nil
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

// NewBlockStore creates a new blockstore backed by the given
// badger.Store and a renterd node
func NewBlockStore(store Store, renterd config.Renterd, log *zap.Logger) *RenterdBlockStore {
	return &RenterdBlockStore{
		store:   store,
		renterd: renterd,
		log:     log,
	}
}
