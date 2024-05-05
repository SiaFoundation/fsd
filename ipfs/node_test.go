package ipfs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	levelds "github.com/ipfs/go-ds-leveldb"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/crypto"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/persist/sqlite"
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

func TestDownload(t *testing.T) {
	log := zaptest.NewLogger(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	privateKey, _, err := crypto.GenerateEd25519Key(frand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := levelds.NewDatastore(filepath.Join(t.TempDir(), "fsdds.leveldb"), nil)
	if err != nil {
		t.Fatal("failed to open leveldb datastore", zap.Error(err))
	}
	defer ds.Close()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "fsd.sqlite3"), log.Named("sqlite"))
	if err != nil {
		log.Fatal("failed to open sqlite database", zap.Error(err))
	}
	defer db.Close()

	bs := &memoryBlockStore{
		blocks: make(map[cid.Cid][]byte),
	}

	node, err := ipfs.NewNode(ctx, privateKey, config.IPFS{}, db, ds, bs, log.Named("ipfs"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	time.Sleep(time.Second)

	t.Log(node.PeerID())
	t.Log(node.Peers())

	path := strings.Split("1002 - Game AIs/1002 - Game AIs.png", "/")
	c := cid.MustParse("QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm")

	downloadCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	r, err := node.DownloadUnixFile(downloadCtx, c, path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		t.Fatal(err)
	}

	shaChecksum := hex.EncodeToString(h.Sum(nil))
	if shaChecksum != "bb783b7b53f4a36fd6076fbc8384ca860c20aecd6e57f29cb23ea06409808f31" {
		t.Fatalf("unexpected hash: %x", h.Sum(nil))
	}
}

func TestDownload2(t *testing.T) {
	log := zaptest.NewLogger(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	privateKey, _, err := crypto.GenerateEd25519Key(frand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := levelds.NewDatastore(filepath.Join(t.TempDir(), "fsdds.leveldb"), nil)
	if err != nil {
		t.Fatal("failed to open leveldb datastore", zap.Error(err))
	}
	defer ds.Close()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "fsd.sqlite3"), log.Named("sqlite"))
	if err != nil {
		log.Fatal("failed to open sqlite database", zap.Error(err))
	}
	defer db.Close()

	bs := &memoryBlockStore{
		blocks: make(map[cid.Cid][]byte),
	}

	node, err := ipfs.NewNode(ctx, privateKey, config.IPFS{}, db, ds, bs, log.Named("ipfs"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	time.Sleep(time.Second)

	t.Log(node.PeerID())
	t.Log(node.Peers())

	c := cid.MustParse("QmawceGscqN4o8Y8Fv26UUmB454kn2bnkXV5tEQYc4jBd6")

	downloadCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	r, err := node.DownloadUnixFile(downloadCtx, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		t.Fatal(err)
	}

	shaChecksum := hex.EncodeToString(h.Sum(nil))
	if shaChecksum != "f0fe7b43114786cf72649551290761a01b27cd85d9f1a97da7f58c2b505d4cf3" {
		t.Fatalf("unexpected hash: %x", h.Sum(nil))
	}
}
