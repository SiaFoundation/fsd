package ipfs

import (
	"errors"
	"fmt"
	"io"
	"sort"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/merkledag/dagutils"
	"github.com/ipfs/boxo/ipld/unixfs"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	multihash "github.com/multiformats/go-multihash/core"
)

var ErrNotFound = errors.New("not found")

type Block struct {
	CID    cid.Cid   `json:"cid"`
	Key    string    `json:"key"`
	Offset uint64    `json:"offset"`
	Length uint64    `json:"length"`
	Links  []cid.Cid `json:"links"`
}

// BuildBalancedCID builds a balanced CID from the given reader It returns a
// slice of Block which contains the CID, the renterd object key, the offset,
// the length and the links of each block.
func BuildBalancedCID(key string, r io.Reader) ([]Block, error) {
	prefix, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get prefix: %w", err)
	}

	prefix.MhType = multihash.SHA2_256

	spl := chunker.NewSizeSplitter(r, chunker.DefaultBlockSize)
	dbp := ihelpers.DagBuilderParams{
		Maxlinks: ihelpers.DefaultLinksPerBlock,
		Dagserv:  dagutils.NewMemoryDagService(),
	}
	db, err := dbp.New(spl)
	if err != nil {
		return nil, fmt.Errorf("failed to create dag builder: %w", err)
	}

	var offset uint64
	blocks := make(map[string]Block)

	var fillNode func(db *ihelpers.DagBuilderHelper, node *ihelpers.FSNodeOverDag, depth int) (ipld.Node, uint64, error)
	fillNode = func(db *ihelpers.DagBuilderHelper, node *ihelpers.FSNodeOverDag, depth int) (ipld.Node, uint64, error) {
		if node == nil {
			node = db.NewFSNodeOverDag(unixfs.TFile)
		}

		var child ipld.Node
		var childSize uint64

		for node.NumChildren() < db.Maxlinks() && !db.Done() {
			if depth == 1 {
				child, childSize, err = db.NewLeafDataNode(unixfs.TFile)
				if err != nil {
					return nil, 0, fmt.Errorf("failed to create new leaf node: %w", err)
				}
				cid := child.Cid()
				blocks[cid.String()] = Block{
					CID:    cid,
					Key:    key,
					Offset: offset,
					Length: childSize,
				}
				offset += childSize
			} else {
				child, childSize, err = fillNode(db, nil, depth-1)
				if err != nil {
					return nil, 0, fmt.Errorf("failed to fill node: %w", err)
				}
				cc := child.Cid()
				var links []cid.Cid
				for _, link := range child.Links() {
					links = append(links, link.Cid)
				}
				size, err := child.Size()
				if err != nil {
					return nil, 0, fmt.Errorf("failed to get size: %w", err)
				}
				blocks[cc.String()] = Block{
					CID:    cc,
					Key:    key,
					Offset: offset,
					Length: size,
					Links:  links,
				}
			}

			if err = node.AddChild(child, childSize, db); err != nil {
				return nil, 0, fmt.Errorf("failed to add child: %w", err)
			}
		}

		nodeSize := node.FileSize()
		filledNode, err := node.Commit()
		return filledNode, nodeSize, err
	}

	root, size, err := db.NewLeafDataNode(unixfs.TFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create new leaf node: %w", err)
	}
	rc := root.Cid()
	blocks[rc.String()] = Block{
		CID:    rc,
		Key:    key,
		Offset: 0,
		Length: size,
	}
	offset += size

	for depth := 1; !db.Done(); depth++ {
		newRoot := db.NewFSNodeOverDag(unixfs.TFile)
		if err := newRoot.AddChild(root, size, db); err != nil {
			return nil, fmt.Errorf("failed to add child: %w", err)
		}

		root, size, err = fillNode(db, newRoot, depth)
		if err != nil {
			return nil, fmt.Errorf("failed to fill node: %w", err)
		}
	}

	var links []cid.Cid
	for _, link := range root.Links() {
		links = append(links, link.Cid)
	}
	rc = root.Cid()
	blocks[rc.String()] = Block{
		CID:    rc,
		Key:    key,
		Offset: 0,
		Length: size,
		Links:  links,
	}

	var blockSlice = make([]Block, 0, len(blocks))
	for _, block := range blocks {
		blockSlice = append(blockSlice, block)
	}
	sort.Slice(blockSlice, func(i, j int) bool {
		if blockSlice[i].Offset < blockSlice[j].Offset {
			return true
		}
		if blockSlice[i].Offset > blockSlice[j].Offset {
			return false
		}
		return len(blockSlice[j].Links) < len(blockSlice[i].Links)
	})
	return blockSlice, nil
}
