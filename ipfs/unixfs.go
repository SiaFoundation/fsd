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
	"go.uber.org/zap"
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

func traverseUnixFSNode(ctx context.Context, ng format.DAGService, parent format.Node, path []string) (format.Node, error) {
	if len(path) == 0 {
		return parent, nil
	}

	dir, err := fsio.NewDirectoryFromNode(ng, parent)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory from node: %w", err)
	}

	child, err := dir.Find(ctx, path[0])
	if err != nil {
		return nil, fmt.Errorf("failed to find child %q: %w", path[0], err)
	}
	return traverseUnixFSNode(ctx, ng, child, path[1:])
}

// DownloadUnixFile downloads a UnixFS CID from IPFS
func (n *Node) DownloadUnixFile(ctx context.Context, c cid.Cid, path []string) (io.ReadSeekCloser, error) {
	dagSess := merkledag.NewSession(ctx, n.dagService)

	rootNode, err := dagSess.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get root node: %w", err)
	}

	node, err := traverseUnixFSNode(ctx, n.dagService, rootNode, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get root node: %w", err)
	}

	dr, err := fsio.NewDagReader(ctx, node, dagSess)
	return dr, err
}

// ServeUnixFile is a special case of DownloadUnixFile for single-page webapps
// that serves the index file if the cid is a directory or the path is not found
func (n *Node) ServeUnixFile(ctx context.Context, c cid.Cid, path []string) (io.ReadSeekCloser, string, error) {
	dagSess := merkledag.NewSession(ctx, n.dagService)

	rootNode, err := dagSess.Get(ctx, c)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get root node: %w", err)
	}

	var indexFileCid *cid.Cid
	for _, link := range rootNode.Links() {
		if link.Name == "index.html" {
			indexFileCid = &link.Cid
			break
		}
	}

	serveIndex := func() (io.ReadSeekCloser, string, error) {
		node, err := dagSess.Get(ctx, *indexFileCid)
		if err != nil {
			return nil, "", fmt.Errorf("failed to get index file: %w", err)
		}
		dr, err := fsio.NewDagReader(ctx, node, dagSess)
		return dr, "index.html", err
	}

	child, err := traverseUnixFSNode(ctx, n.dagService, rootNode, path)
	if err != nil && strings.Contains(err.Error(), "failed to resolve path") {
		if indexFileCid != nil {
			return serveIndex()
		}
	} else if err != nil {
		return nil, "", fmt.Errorf("failed to traverse node: %w", err)
	}

	// if the resolved node is a directory and the index file exists
	// serve it.
	dir, err := fsio.NewDirectoryFromNode(n.dagService, child)
	if err == nil {
		n.log.Debug("serving index.html")
		child, err := dir.Find(ctx, "index.html")
		if err != nil {
			return nil, "", fmt.Errorf("failed to find index.html: %w", err)
		}
		dr, err := fsio.NewDagReader(ctx, child, dagSess)
		return dr, "index.html", err
	} else {
		n.log.Debug("failed to create directory from node", zap.Error(err))
	}

	dr, err := fsio.NewDagReader(ctx, child, dagSess)
	return dr, path[len(path)-1], err
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
	}
	n.reprovider.Trigger()
	return rootNode, nil
}
