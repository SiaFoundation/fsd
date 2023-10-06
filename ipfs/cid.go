package ipfs

import (
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

type CIDMap struct {
	CID    cid.Cid   `json:"cid"`
	Key    string    `json:"key"`
	Offset uint64    `json:"offset"`
	Length uint64    `json:"length"`
	Links  []cid.Cid `json:"links"`
}

func BuildBalancedCID(key string, r io.Reader) ([]CIDMap, error) {
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

	var blocks []CIDMap

	var offset uint64
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
				blocks = append(blocks, CIDMap{
					CID:    cid,
					Key:    key,
					Offset: offset,
					Length: childSize,
				})
				offset += childSize
			} else {
				child, childSize, err = fillNode(db, nil, depth-1)
				if err != nil {
					return nil, 0, fmt.Errorf("failed to fill node: %w", err)
				}

				var links []cid.Cid
				for _, link := range child.Links() {
					links = append(links, link.Cid)
				}

				blocks = append(blocks, CIDMap{
					CID:    child.Cid(),
					Key:    key,
					Offset: offset,
					Length: childSize,
					Links:  links,
				})
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
	blocks = append(blocks, CIDMap{
		CID:    root.Cid(),
		Key:    key,
		Offset: 0,
		Length: size,
	})
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
		var links []cid.Cid
		for _, link := range root.Links() {
			links = append(links, link.Cid)
		}

		blocks = append(blocks, CIDMap{
			CID:    root.Cid(),
			Key:    key,
			Offset: offset,
			Length: size,
			Links:  links,
		})
	}

	var links []cid.Cid
	for _, link := range root.Links() {
		links = append(links, link.Cid)
	}
	cid := root.Cid()
	rootNode := CIDMap{
		CID:    cid,
		Key:    key,
		Offset: 0,
		Length: size,
		Links:  links,
	}
	blocks = append(blocks, rootNode)

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})
	return blocks, nil
}
