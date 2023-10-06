package blockstore

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

type Exchange struct {
	bserv *BlockService
}

func (e *Exchange) Close() error {
	return nil
}

// GetBlock returns the block associated with a given cid.
func (e *Exchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return e.bserv.GetBlock(ctx, c)
}

// GetBlocks returns the blocks associated with the given cids.
// If the requested blocks are not found immediately, this function should hang until
// they are found. If they can't be found later, it's also acceptable to terminate.
func (e *Exchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	return e.bserv.GetBlocks(ctx, cids), nil
}

// NotifyNewBlocks tells the exchange that new blocks are available and can be served.
func (e *Exchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil // TODO
}
