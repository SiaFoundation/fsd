package ipfs_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/crypto"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/persist/badger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type memoryBlockStore struct {
	mu     sync.Mutex
	blocks map[cid.Cid][]byte
}

// DeleteBlock removes a given block from the blockstore.
// note: this is not implemented
func (ms *memoryBlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.blocks, c)
	return nil
}

// Has returns whether or not a given block is in the blockstore.
func (ms *memoryBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	_, ok := ms.blocks[c]
	return ok, nil
}

// Get returns a block by CID
func (ms *memoryBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if b, ok := ms.blocks[c]; ok {
		return blocks.NewBlockWithCid(b, c)
	}
	return nil, format.ErrNotFound{Cid: c}
}

// GetSize returns the CIDs mapped BlockSize
func (ms *memoryBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if b, ok := ms.blocks[c]; ok {
		return len(b), nil
	}
	return 0, format.ErrNotFound{Cid: c}
}

// Put puts a given block to the underlying datastore
func (ms *memoryBlockStore) Put(ctx context.Context, b blocks.Block) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.blocks[b.Cid()] = b.RawData()
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (ms *memoryBlockStore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	for _, b := range blocks {
		if err := ms.Put(ctx, b); err != nil {
			return err
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
func (ms *memoryBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	go func() {
		var keys []cid.Cid
		ms.mu.Lock()
		for c := range ms.blocks {
			keys = append(keys, c)
		}
		ms.mu.Unlock()

		for _, c := range keys {
			select {
			case <-ctx.Done():
				return
			case ch <- c:
			}
		}
		close(ch)
	}()

	return ch, nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (ms *memoryBlockStore) HashOnRead(enabled bool) {
	// TODO: implement
}

func TestTest(t *testing.T) {
	log := zaptest.NewLogger(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	privateKey, _, err := crypto.GenerateEd25519Key(frand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	store, err := badger.OpenDatabase(filepath.Join(t.TempDir(), "fsd.badgerdb"), log.Named("badger"))
	if err != nil {
		log.Fatal("failed to open badger database", zap.Error(err))
	}
	defer store.Close()

	memBlockStore := &memoryBlockStore{
		blocks: make(map[cid.Cid][]byte),
	}

	node, err := ipfs.NewNode(ctx, privateKey, config.IPFS{}, memBlockStore)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	time.Sleep(5 * time.Second)

	t.Log(node.PeerID())
	t.Log(node.Peers())

	c := cid.MustParse("QmawceGscqN4o8Y8Fv26UUmB454kn2bnkXV5tEQYc4jBd6")

	downloadCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	r, err := node.DownloadCID(downloadCtx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	f, err := os.Open("test.png")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		t.Fatal(err)
	}
}
