package renterd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
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

	start := time.Now()
	if err := bs.busClient.DeleteObject(ctx, bs.bucket, key, api.DeleteObjectOptions{}); err != nil {
		log.Debug("failed to delete block", zap.Error(err))
	}
	log.Debug("deleted block", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// Has returns whether or not a given block is in the blockstore.
func (bs *BlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	key := cidKey(c)
	log := bs.log.Named("Has").With(zap.Stringer("cid", c), zap.String("key", key))

	start := time.Now()
	_, err := bs.busClient.Object(ctx, bs.bucket, key, api.GetObjectOptions{})
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
	if err != nil {
		if !strings.Contains(err.Error(), "block not found") {
			bs.log.Error("failed to download block", zap.Stringer("cid", c), zap.Error(err))
		}
		return nil, format.ErrNotFound{Cid: c}
	}
	return block, nil
}

// GetSize returns the CIDs mapped BlockSize
func (bs *BlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	key := cidKey(c)
	log := bs.log.Named("GetSize").With(zap.Stringer("cid", c), zap.String("key", key))
	stat, err := bs.busClient.Object(ctx, bs.bucket, key, api.GetObjectOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "object not found") {
			log.Debug("failed to get block size", zap.Error(err))
		}
		return 0, format.ErrNotFound{Cid: c}
	}
	log.Debug("got block size", zap.Int("size", int(stat.Object.Size)))
	return int(stat.Object.Size), nil
}

// Put puts a given block to the underlying datastore
func (bs *BlockStore) Put(ctx context.Context, b blocks.Block) error {
	key := cidKey(b.Cid())
	log := bs.log.Named("Put").With(zap.Stringer("cid", b.Cid()), zap.String("key", key), zap.Int("size", len(b.RawData())))
	start := time.Now()
	_, err := bs.workerClient.UploadObject(ctx, bytes.NewReader(b.RawData()), bs.bucket, key, api.UploadObjectOptions{})
	log.Debug("put block", zap.Duration("duration", time.Since(start)), zap.Error(err))
	return err
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
		var marker string
		for {
			resp, err := bs.busClient.ListObjects(ctx, bs.bucket, api.ListObjectOptions{
				Marker: marker,
				Limit:  100,
			})
			if err != nil {
				close(ch)
				return
			} else if len(resp.Objects) == 0 {
				close(ch)
				return
			}

			for _, obj := range resp.Objects {
				name := obj.Name
				if p := strings.Split(obj.Name, "/"); len(p) > 1 {
					name = p[len(p)-1]
				}
				c, err := cid.Parse(name)
				if err != nil {
					log.Debug("skipping invalid key", zap.String("key", obj.Name), zap.String("name", name))
					continue
				}
				log.Debug("found key", zap.Stringer("cid", c))
				select {
				case ch <- c:
				case <-ctx.Done():
					close(ch)
					return
				}
				marker = obj.Name
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

	if options.Bus == nil {
		return nil, fmt.Errorf("bus client is required")
	} else if options.Worker == nil {
		return nil, fmt.Errorf("worker client is required")
	} else if options.Downloader == nil {
		return nil, fmt.Errorf("block downloader is required")
	}

	return &BlockStore{
		log:          options.Log,
		busClient:    options.Bus,
		workerClient: options.Worker,
		bucket:       options.Bucket,
		downloader:   options.Downloader,
	}, nil
}
