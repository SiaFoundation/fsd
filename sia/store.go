package sia

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	bs.log.Debug("has block", zap.Stringer("cid", c))
	return bs.store.HasBlock(ctx, c)
}

func (bs *RenterdBlockStore) downloadBlock(ctx context.Context, cm Block) (blocks.Block, error) {
	var data, metadata []byte
	var err error

	if cm.Data.BlockSize != 0 {
		data, err = downloadPartialData(ctx, bs.renterd, cm.Data.Bucket, cm.Data.Key, cm.Data.Offset, cm.Data.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to download data: %w", err)
		}
	}

	if cm.Metadata.Length != 0 {
		metadata, err = downloadPartialData(ctx, bs.renterd, cm.Metadata.Bucket, cm.Metadata.Key, cm.Metadata.Offset, cm.Metadata.Length)
		if err != nil {
			return nil, fmt.Errorf("failed to download object: %w", err)
		}
	}

	if n := len(data); n != int(cm.Data.BlockSize) {
		return nil, fmt.Errorf("unexpected data size: requested %d got %d", cm.Data.BlockSize, n)
	} else if n := len(metadata); n != int(cm.Metadata.Length) {
		return nil, fmt.Errorf("unexpected metadata size: requested %d got %d", cm.Metadata.Length, n)
	}

	var meta pb.Data
	if err := proto.Unmarshal(metadata, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// note: non-nil block data encodes to 2 bytes
	if cm.Data.BlockSize != 0 {
		meta.Data = data
	}

	buf, err := proto.Marshal(&meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	node := merkledag.NodeWithData(buf)
	for _, link := range cm.Links {
		node.AddRawLink(link.Name, &format.Link{
			Name: link.Name,
			Size: link.Size,
			Cid:  link.CID,
		})
	}
	return node, nil
}

// Get returns a block by CID
func (bs *RenterdBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	cm, err := bs.store.GetBlock(ctx, c)
	if errors.Is(err, ErrNotFound) {
		return nil, format.ErrNotFound{Cid: c}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get cid: %w", err)
	}

	log := bs.log.With(zap.Stringer("cid", c), zap.String("dataBucket", cm.Data.Bucket), zap.String("dataKey", cm.Data.Key), zap.String("metaBucket", cm.Metadata.Bucket), zap.String("metaKey", cm.Metadata.Key), zap.Uint64("blockSize", cm.Data.BlockSize), zap.Uint64("blockOffset", cm.Data.Offset), zap.Uint64("metadataSize", cm.Metadata.Length), zap.Uint64("metadataOffset", cm.Metadata.Offset))
	log.Debug("downloading block")

	start := time.Now()
	block, err := bs.downloadBlock(ctx, cm)
	if err != nil {
		log.Error("failed to download block", zap.Error(err))
	}
	log.Debug("block downloaded", zap.Duration("elapsed", time.Since(start)))
	return block, err
}

// GetSize returns the CIDs mapped BlockSize
func (bs *RenterdBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	cm, err := bs.store.GetBlock(ctx, c)
	if errors.Is(err, ErrNotFound) {
		return 0, format.ErrNotFound{Cid: c}
	} else if err != nil {
		return 0, fmt.Errorf("failed to get cid: %w", err)
	}
	bs.log.Debug("get block size", zap.Stringer("cid", c), zap.Uint64("size", cm.Data.BlockSize))
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
