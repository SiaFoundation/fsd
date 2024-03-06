package ipfs

import (
	"context"
	"fmt"
	"io"
	"strings"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	fsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

type (
	unixFSOptions struct {
		CIDBuilder cid.Builder
		RawLeaves  bool
		MaxLinks   int
		BlockSize  int64
	}
	// A UnixFSOption sets options for the UnixFS importer
	UnixFSOption func(*unixFSOptions)
)

// UnixFSWithCIDBuilder sets the CID builder for the UnixFS importer
func UnixFSWithCIDBuilder(b cid.Builder) UnixFSOption {
	return func(u *unixFSOptions) {
		u.CIDBuilder = b
	}
}

// UnixFSWithRawLeaves sets the raw leaves option for the UnixFS importer
func UnixFSWithRawLeaves(b bool) UnixFSOption {
	return func(u *unixFSOptions) {
		u.RawLeaves = b
	}
}

// UnixFSWithMaxLinks sets the maximum number of links per block for the UnixFS importer
func UnixFSWithMaxLinks(b int) UnixFSOption {
	return func(u *unixFSOptions) {
		u.MaxLinks = b
	}
}

// UnixFSWithBlockSize sets the block size for the UnixFS importer
func UnixFSWithBlockSize(b int64) UnixFSOption {
	return func(u *unixFSOptions) {
		u.BlockSize = b
	}
}

// DownloadUnixFile downloads a UnixFS CID from IPFS
func (n *Node) DownloadUnixFile(ctx context.Context, c cid.Cid, path []string) (io.ReadSeekCloser, error) {
	dagSess := merkledag.NewSession(ctx, n.dagService)
	rootNode, err := dagSess.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get root node: %w", err)
	}

	var traverse func(context.Context, format.Node, []string) (format.Node, error)
	traverse = func(ctx context.Context, parent format.Node, path []string) (format.Node, error) {
		if len(path) == 0 {
			return parent, nil
		}

		childLink, rem, err := parent.Resolve(path)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve path %q: %w", strings.Join(path, "/"), err)
		}

		switch v := childLink.(type) {
		case *format.Link:
			childNode, err := dagSess.Get(ctx, v.Cid)
			if err != nil {
				return nil, fmt.Errorf("failed to get child node %q: %w", v.Cid, err)
			}
			return traverse(ctx, childNode, rem)
		default:
			return nil, fmt.Errorf("expected link node, got %T", childLink)
		}
	}

	node, err := traverse(ctx, rootNode, path)
	if err != nil {
		return nil, fmt.Errorf("failed to traverse path: %w", err)
	}

	dr, err := fsio.NewDagReader(ctx, node, dagSess)
	return dr, err
}

// UploadUnixFile uploads a UnixFS file to IPFS
func (n *Node) UploadUnixFile(ctx context.Context, r io.Reader, opts ...UnixFSOption) (format.Node, error) {
	opt := unixFSOptions{
		MaxLinks:  ihelpers.DefaultLinksPerBlock,
		BlockSize: chunker.DefaultBlockSize,
	}
	for _, o := range opts {
		o(&opt)
	}

	params := ihelpers.DagBuilderParams{
		Dagserv:    n.dagService,
		CidBuilder: opt.CIDBuilder,
		RawLeaves:  opt.RawLeaves,
		Maxlinks:   opt.MaxLinks,
	}

	spl := chunker.NewSizeSplitter(r, opt.BlockSize)
	db, err := params.New(spl)
	if err != nil {
		return nil, fmt.Errorf("failed to create dag builder: %w", err)
	}

	rootNode, err := balanced.Layout(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create balanced layout: %w", err)
	} else if err := n.provider.Provide(rootNode.Cid()); err != nil {
		return nil, fmt.Errorf("failed to provide root node: %w", err)
	}

	return rootNode, nil
}
