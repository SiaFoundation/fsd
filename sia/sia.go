package sia

import (
	"context"
	"encoding/hex"
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
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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
		// Provide broadcasts a CID to the IPFS network
		Provide(cid.Cid) error
		// GetBlock retrieves a block from the IPFS network
		GetBlock(context.Context, cid.Cid) (blocks.Block, error)
	}

	// A Node is a specialized IPFS gateway that retrieves data from a renterd
	// node
	Node struct {
		store Store
		ipfs  IPFSProvider
		log   *zap.Logger

		worker *worker.Client
		bus    *bus.Client

		renterd config.Renterd
	}

	// UnixFSOptions holds configuration options for UnixFS uploads
	UnixFSOptions struct {
		CIDBuilder cid.Builder
		RawLeaves  bool
		MaxLinks   int
		BlockSize  int64
	}
)

// ErrNotFound is returned when a CID is not found in the store
var ErrNotFound = errors.New("not found")

func setDefaultCIDOpts(opts *UnixFSOptions) {
	if opts.MaxLinks <= 0 {
		opts.MaxLinks = ihelpers.DefaultLinksPerBlock
	}

	if opts.BlockSize <= 0 {
		opts.BlockSize = chunker.DefaultBlockSize
	}
}

// UploadFile uploads a unixfs file to the renterd node and returns the root CID
func (n *Node) UploadFile(ctx context.Context, r io.Reader, opts UnixFSOptions) (cid.Cid, error) {
	log := n.log.Named("upload").With(zap.String("bucket", n.renterd.Bucket))

	uploadID := hex.EncodeToString(frand.Bytes(32))
	tmpDataKey := uploadID + ".tmp"
	tmpMetaKey := uploadID + ".meta.tmp"

	metaR, metaW := io.Pipe()
	dataR, dataW := io.Pipe()

	dagSvc := NewUnixFileUploader(dataW, metaW, log)

	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer metaW.Close()

		if _, err := n.worker.UploadObject(ctx, dataR, n.renterd.Bucket, tmpDataKey, api.UploadObjectOptions{}); err != nil {
			errCh <- fmt.Errorf("failed to upload data: %w", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		defer dataW.Close()

		if _, err := n.worker.UploadObject(ctx, metaR, n.renterd.Bucket, tmpMetaKey, api.UploadObjectOptions{}); err != nil {
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
		return cid.Cid{}, fmt.Errorf("failed to create dag builder: %w", err)
	}

	rootNode, err := balanced.Layout(db)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to build dag: %w", err)
	}

	// close the pipes to signal the uploaders that we're done
	metaW.Close()
	dataW.Close()
	// wait for uploads to finish
	wg.Wait()

	select {
	case err := <-errCh:
		return cid.Cid{}, err
	default:
	}

	dataKey := rootNode.Cid().String()
	metaKey := dataKey + ".meta"

	if err = n.bus.RenameObject(ctx, n.renterd.Bucket, "/"+tmpDataKey, "/"+dataKey, true); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to rename tmp data: %w", err)
	} else if err = n.bus.RenameObject(ctx, n.renterd.Bucket, "/"+tmpMetaKey, "/"+metaKey, true); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to rename tmp metadata: %w", err)
	}

	// get the blocks from the dag service
	blocks := dagSvc.Blocks()
	// set the renterd bucket and object key for the blocks
	for i := range blocks {
		blocks[i].Data.Bucket = n.renterd.Bucket
		blocks[i].Data.Key = dataKey
		blocks[i].Metadata.Bucket = n.renterd.Bucket
		blocks[i].Metadata.Key = metaKey
	}

	// add the blocks to the store
	if err := n.store.AddBlocks(ctx, blocks); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to add blocks to store: %w", err)
	}

	// broadcast the CIDs to the IPFS network
	for _, b := range blocks {
		if err := n.ipfs.Provide(b.CID); err != nil {
			return cid.Cid{}, fmt.Errorf("failed to provide block: %w", err)
		}
	}
	return rootNode.Cid(), nil
}

// ProxyHTTPDownload proxies an http download request to the renterd node
func (n *Node) ProxyHTTPDownload(c cid.Cid, r *http.Request, w http.ResponseWriter) error {
	block, err := n.store.GetBlock(r.Context(), c)
	if err != nil {
		return err
	} else if block.Data.Offset != 0 {
		return errors.New("cannot proxy partial downloads")
	}

	target, err := url.Parse(n.renterd.WorkerAddress + "/objects/" + block.Data.Key)
	if err != nil {
		panic(err)
	}
	target.RawQuery = url.Values{
		"bucket": []string{block.Data.Bucket},
	}.Encode()

	rp := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.Out.Method = http.MethodGet
			r.Out.URL = target
			r.Out.SetBasicAuth("", n.renterd.WorkerPassword)
			r.Out.Header.Set("Range", r.In.Header.Get("Range"))

			n.log.Debug("proxying request to", zap.Stringer("url", r.Out.URL))
		},
	}

	rp.ServeHTTP(w, r)
	return nil
}

// CalculateBlocks calculates the blocks for a given reader and returns them
func (n *Node) CalculateBlocks(ctx context.Context, r io.Reader, opts UnixFSOptions) ([]Block, error) {
	dagSvc := NewUnixFileUploader(io.Discard, io.Discard, n.log.Named("calculate"))

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

		worker: worker.NewClient(cfg.WorkerAddress, cfg.WorkerPassword),
		bus:    bus.NewClient(cfg.BusAddress, cfg.BusPassword),

		renterd: cfg,
	}
}
