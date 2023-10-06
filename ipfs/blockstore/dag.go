package blockstore

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

type BlockService struct {
	store blockstore.Blockstore
}

func (bs *BlockService) Close() error {
	return nil
}

// GetBlock gets the requested block.
func (bs *BlockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return bs.store.Get(ctx, c)
}

// GetBlocks does a batch request for the given cids, returning blocks as
// they are found, in no particular order.
//
// It may not be able to find all requested blocks (or the context may
// be canceled). In that case, it will close the channel early. It is up
// to the consumer to detect this situation and keep track which blocks
// it has received and which it hasn't.
func (bs *BlockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		for _, k := range ks {
			b, err := bs.store.Get(ctx, k)
			if err != nil {
				continue
			}
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// Blockstore returns a reference to the underlying blockstore
func (bs *BlockService) Blockstore() blockstore.Blockstore {
	return bs.store
}

// Exchange returns a reference to the underlying exchange (usually bitswap)
func (bs *BlockService) Exchange() exchange.Interface {
	return &Exchange{
		bserv: bs,
	}
}

// AddBlock puts a given block to the underlying datastore
func (bs *BlockService) AddBlock(ctx context.Context, o blocks.Block) error {
	return errors.New("cannot put blocks")
}

// AddBlocks adds a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *BlockService) AddBlocks(ctx context.Context, b []blocks.Block) error {
	return errors.New("cannot put blocks")
}

// DeleteBlock deletes the given block from the blockservice.
func (bs *BlockService) DeleteBlock(ctx context.Context, o cid.Cid) error {
	return errors.New("cannot delete blocks")
}

// NewBlockstoreService returns a new BlockService backed by the given blockstore.
func NewBlockstoreService(bs blockstore.Blockstore) *BlockService {
	return &BlockService{
		store: bs,
	}
}
