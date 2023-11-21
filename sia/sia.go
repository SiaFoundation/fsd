package sia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/fsd/config"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

type (
	// A Store is a persistent store for IPFS blocks
	Store interface {
		AddBlocks(context.Context, []Block) error
		GetBlock(ctx context.Context, c cid.Cid) (Block, error)
		HasBlock(ctx context.Context, c cid.Cid) (bool, error)
		AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
	}

	// An IPFSProvider broadcasts CIDs to the IPFS network
	IPFSProvider interface {
		Provide(cid.Cid) error
	}

	// A Node is a specialized IPFS gateway that retrieves data from a renterd
	// node
	Node struct {
		store Store
		ipfs  IPFSProvider
		log   *zap.Logger

		renterd config.Renterd
	}

	CIDOptions struct {
		CIDBuilder cid.Builder
		RawLeaves  bool
		MaxLinks   int
		BlockSize  int64
	}
)

// ErrNotFound is returned when a CID is not found in the store
var ErrNotFound = errors.New("not found")

func setDefaultCIDOpts(opts *CIDOptions) {
	if opts.MaxLinks <= 0 {
		opts.MaxLinks = ihelpers.DefaultLinksPerBlock
	}

	if opts.BlockSize <= 0 {
		opts.BlockSize = chunker.DefaultBlockSize
	}
}

// UploadCID uploads a CID to the renterd node
func (n *Node) UploadCID(ctx context.Context, c cid.Cid, r io.Reader, opts CIDOptions) error {
	log := n.log.Named("upload").With(zap.Stringer("cid", c), zap.String("bucket", n.renterd.Bucket))

	dataKey := c.String()
	metaKey := dataKey + ".meta"

	metaR, metaW := io.Pipe()
	dataR, dataW := io.Pipe()

	client := worker.NewClient(n.renterd.Address, n.renterd.Password)
	dagSvc := NewUnixFileUploader(c.String(), dataW, metaW, log)

	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer metaW.Close()

		if _, err := client.UploadObject(ctx, dataR, n.renterd.Bucket, dataKey, api.UploadObjectOptions{}); err != nil {
			errCh <- fmt.Errorf("failed to upload data: %w", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		defer dataW.Close()

		if _, err := client.UploadObject(ctx, metaR, n.renterd.Bucket, metaKey, api.UploadObjectOptions{}); err != nil {
			errCh <- fmt.Errorf("failed to upload metadata: %w", err)
			return
		}
	}()

	setDefaultCIDOpts(&opts)
	spl := chunker.NewSizeSplitter(r, opts.BlockSize)
	dbp := ihelpers.DagBuilderParams{
		Dagserv:    dagSvc,
		CidBuilder: opts.CIDBuilder,
		RawLeaves:  opts.RawLeaves,
		Maxlinks:   opts.MaxLinks,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return fmt.Errorf("failed to create dag builder: %w", err)
	}

	rootNode, err := balanced.Layout(db)
	if err != nil {
		return fmt.Errorf("failed to build dag: %w", err)
	}

	// close the pipes to signal the uploaders that we're done
	metaW.Close()
	dataW.Close()
	// wait for uploads to finish
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	blocks := dagSvc.Blocks()

	if !rootNode.Cid().Equals(c) {
		return fmt.Errorf("unexpected root cid: %s", rootNode.Cid().String())
	} else if err := n.store.AddBlocks(ctx, blocks); err != nil {
		return fmt.Errorf("failed to add blocks to store: %w", err)
	}

	for _, b := range blocks {
		if err := n.ipfs.Provide(b.CID); err != nil {
			return fmt.Errorf("failed to provide block: %w", err)
		}
	}
	return nil
}

func (n *Node) getBlock(ctx context.Context, c cid.Cid) (Block, error) {
	block, err := n.store.GetBlock(ctx, c)
	if errors.Is(err, ErrNotFound) {
		switch c.Version() { // check if we can find the block by converting the cid version
		case 0:
			v1Cid := cid.NewCidV1(c.Type(), c.Hash())
			return n.store.GetBlock(ctx, v1Cid)
		case 1:
			h := c.Hash()
			dec, err := multihash.Decode(h)
			if err != nil {
				n.log.Debug("failed to decode  v1 multihash", zap.Stringer("cid", c), zap.Error(err))
				return Block{}, ErrNotFound
			}
			if dec.Code != multihash.SHA2_256 || dec.Length != 32 {
				n.log.Debug("cannot convert v1 CID to v0", zap.Stringer("cid", c), zap.String("code", multihash.Codes[dec.Code]), zap.Int("length", dec.Length))
				return Block{}, ErrNotFound
			}
			v0Cid := cid.NewCidV0(h)
			return n.store.GetBlock(ctx, v0Cid)
		}
		return Block{}, ErrNotFound
	}
	return block, err
}

// ProxyHTTPDownload proxies an http download request to the renterd node
func (n *Node) ProxyHTTPDownload(cid cid.Cid, r *http.Request, w http.ResponseWriter) error {
	block, err := n.getBlock(r.Context(), cid)
	if err != nil {
		return err
	} else if block.Data.Offset != 0 {
		return errors.New("cannot proxy partial downloads")
	}

	target, err := url.Parse(n.renterd.Address + "/objects/" + block.Data.Key)
	if err != nil {
		panic(err)
	}
	target.RawQuery = url.Values{
		"bucket": []string{n.renterd.Bucket},
	}.Encode()

	rp := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.Out.Method = http.MethodGet
			r.Out.URL = target
			r.Out.SetBasicAuth("", n.renterd.Password)
			r.Out.Header.Set("Range", r.In.Header.Get("Range"))

			n.log.Debug("proxying request to", zap.Stringer("url", r.Out.URL))
		},
	}

	rp.ServeHTTP(w, r)
	return nil
}

// CalculateBlocks calculates the blocks for a given reader and returns them
func (n *Node) CalculateBlocks(ctx context.Context, r io.Reader, opts CIDOptions) ([]Block, error) {
	dagSvc := NewUnixFileUploader("", io.Discard, io.Discard, n.log.Named("calculate"))

	setDefaultCIDOpts(&opts)
	spl := chunker.NewSizeSplitter(r, opts.BlockSize)
	dbp := ihelpers.DagBuilderParams{
		Dagserv:    dagSvc,
		CidBuilder: opts.CIDBuilder,
		RawLeaves:  opts.RawLeaves,
		Maxlinks:   opts.MaxLinks,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return nil, fmt.Errorf("failed to create dag builder: %w", err)
	}

	_, err = balanced.Layout(db)
	if err != nil {
		return nil, fmt.Errorf("failed to build dag: %w", err)
	}

	return dagSvc.Blocks(), nil
}

// VerifyCID verifies that a CID is correctly stored in the renterd node
func (n *Node) VerifyCID(ctx context.Context, c cid.Cid) error {
	rbs := NewBlockStore(n.store, n.renterd, n.log.Named("verify"))

	block, err := rbs.Get(ctx, c)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	} else if block.Cid().String() != c.String() {
		return fmt.Errorf("unexpected root cid: %s", block.Cid().String())
	}
	return nil
}

// New creates a new Sia IPFS store
func New(store Store, ipfs IPFSProvider, cfg config.Renterd, log *zap.Logger) *Node {
	return &Node{
		store: store,
		ipfs:  ipfs,
		log:   log,

		renterd: cfg,
	}
}
