package renterd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/bus"
	"go.sia.tech/renterd/v2/worker"
	"go.uber.org/zap"
)

type (
	// A BlockDownloader downloads blocks from a renterd node.
	BlockDownloader interface {
		Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
	}

	// A BlockStore is a blockstore backed by a renterd node.
	BlockStore struct {
		log *zap.Logger

		bucket string

		workerClient *worker.Client
		busClient    *bus.Client

		metadata   MetadataStore
		downloader BlockDownloader
	}
)

func cidKey(c cid.Cid) string {
	return cid.NewCidV1(c.Type(), c.Hash()).String()
}

// DeleteBlock removes a given block from the blockstore.
func (bs *BlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	key := cidKey(c)
	log := bs.log.Named("DeleteBlock").With(zap.Stack("stack"), zap.Stringer("cid", c), zap.String("key", key))

	if err := bs.metadata.Unpin(c); err != nil {
		return fmt.Errorf("failed to unpin block: %w", err)
	}

	start := time.Now()
	if err := bs.busClient.DeleteObject(ctx, bs.bucket, key); err != nil {
		log.Debug("failed to delete block", zap.Error(err))
	}
	log.Debug("deleted block", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// Has returns whether or not a given block is in the blockstore.
func (bs *BlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	log := bs.log.Named("Has").With(zap.Stringer("cid", c))

	start := time.Now()

	bucket, key, err := bs.metadata.BlockLocation(c)
	if format.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get block location: %w", err)
	}

	_, err = bs.busClient.Object(ctx, bucket, key, api.GetObjectOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "object not found") {
			log.Debug("block does not exist", zap.Duration("elapsed", time.Since(start)))
			return false, nil
		}
		log.Debug("failed to get block", zap.Error(err))
		return false, fmt.Errorf("failed to check block existence: %w", err)
	}
	log.Debug("block exists", zap.Duration("elapsed", time.Since(start)))
	return true, nil
}

// Get returns a block by CID
func (bs *BlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	block, err := bs.downloader.Get(ctx, c)
	return block, err
}

// GetSize returns the CIDs mapped BlockSize
func (bs *BlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	key := cidKey(c)
	log := bs.log.Named("GetSize").With(zap.Stringer("cid", c), zap.String("key", key))

	bucket, key, err := bs.metadata.BlockLocation(c)
	if err != nil {
		return 0, err
	}

	stat, err := bs.busClient.Object(ctx, bucket, key, api.GetObjectOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "object not found") {
			log.Debug("failed to get block size", zap.Error(err))
		}
		return 0, format.ErrNotFound{Cid: c}
	}
	log.Debug("got block size", zap.Int("size", int(stat.Object.TotalSize())))
	return int(stat.Object.TotalSize()), nil
}

// Put puts a given block to the underlying datastore
func (bs *BlockStore) Put(ctx context.Context, b blocks.Block) error {
	key := cidKey(b.Cid())
	log := bs.log.Named("Put").With(zap.Stringer("cid", b.Cid()), zap.String("key", key), zap.Int("size", len(b.RawData())))
	start := time.Now()
	_, err := bs.workerClient.UploadObject(ctx, bytes.NewReader(b.RawData()), bs.bucket, key, api.UploadObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to store block %q: %w", b.Cid(), err)
	}

	log.Debug("object uploaded", zap.Duration("elapsed", time.Since(start)))

	meta := PinnedBlock{
		Cid:       b.Cid(),
		Bucket:    bs.bucket,
		ObjectKey: key,
	}

	for _, link := range blockLinks(b) {
		meta.Links = append(meta.Links, link.Cid)
	}

	if err := bs.metadata.Pin(meta); err != nil {
		log.Debug("failed to pin block", zap.Error(err))
		return fmt.Errorf("failed to pin block %q: %w", b.Cid(), err)
	}
	log.Debug("put block", zap.Duration("duration", time.Since(start)), zap.Error(err))
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *BlockStore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	log := bs.log.Named("PutMany").With(zap.Int("blocks", len(blocks)))

	for _, block := range blocks {
		log.Debug("putting block", zap.Stringer("cid", block.Cid()))
		if err := bs.Put(ctx, block); err != nil {
			return fmt.Errorf("failed to put block %q: %w", block.Cid(), err)
		}
	}

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
func (bs *BlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	log := bs.log.Named("AllKeysChan")
	ch := make(chan cid.Cid)
	go func() {
		for i := 0; ; i += 1000 {
			cids, err := bs.metadata.Pinned(i, 1000)
			if err != nil {
				bs.log.Error("failed to get root CIDs", zap.Error(err))
				close(ch)
				return
			} else if len(cids) == 0 {
				close(ch)
				return
			}

			log.Debug("got pinned CIDs", zap.Int("count", len(cids)))
			for _, c := range cids {
				select {
				case ch <- c:
				case <-ctx.Done():
					close(ch)
					return
				}

				// since only the v1 CID is stored, try to convert it to v0
				if c.Type() == uint64(multicodec.DagPb) && c.Prefix().MhType == multihash.SHA2_256 {
					cv0 := cid.NewCidV0(c.Hash())
					select {
					case ch <- cv0:
					case <-ctx.Done():
						close(ch)
						return
					}
				}
			}
		}
	}()
	return ch, nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *BlockStore) HashOnRead(enabled bool) {
	// TODO: implement
}

// NewBlockStore creates a new blockstore backed by a renterd node
func NewBlockStore(opts ...Option) (*BlockStore, error) {
	options := &options{
		Log:    zap.NewNop(),
		Bucket: "ipfs",
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.Store == nil {
		return nil, fmt.Errorf("metadata store is required")
	} else if options.Bus == nil {
		return nil, fmt.Errorf("bus client is required")
	} else if options.Worker == nil {
		return nil, fmt.Errorf("worker client is required")
	} else if options.Downloader == nil {
		return nil, fmt.Errorf("block downloader is required")
	}

	return &BlockStore{
		log:          options.Log,
		metadata:     options.Store,
		busClient:    options.Bus,
		workerClient: options.Worker,
		bucket:       options.Bucket,
		downloader:   options.Downloader,
	}, nil
}

func blockLinks(b blocks.Block) []*format.Link {
	pn, err := merkledag.DecodeProtobuf(b.RawData())
	if err != nil {
		return nil
	}
	return pn.Links()
}
