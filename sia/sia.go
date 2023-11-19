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

	// A Node is a specialized IPFS gateway that retrieves data from a renterd
	// node
	Node struct {
		store Store
		log   *zap.Logger

		renterd config.Renterd
	}
)

// ErrNotFound is returned when a CID is not found in the store
var ErrNotFound = errors.New("not found")

// UploadCID uploads a CID to the renterd node
func (n *Node) UploadCID(ctx context.Context, c cid.Cid, r io.Reader) error {
	log := n.log.Named("upload").With(zap.String("cid", c.Hash().B58String()), zap.String("bucket", n.renterd.Bucket))

	dataKey := c.Hash().B58String()
	metaKey := dataKey + ".meta"

	metaR, metaW := io.Pipe()
	dataR, dataW := io.Pipe()

	client := worker.NewClient(n.renterd.Address, n.renterd.Password)
	dagSvc := NewUnixFileUploader(c.Hash().B58String(), dataW, metaW, log)

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

	spl := chunker.NewSizeSplitter(r, chunker.DefaultBlockSize)

	dbp := ihelpers.DagBuilderParams{
		Maxlinks: ihelpers.DefaultLinksPerBlock,
		Dagserv:  dagSvc,
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

	if rootNode.Cid().Hash().B58String() != c.Hash().B58String() {
		return fmt.Errorf("unexpected root cid: %s", rootNode.Cid().Hash().B58String())
	}
	return n.store.AddBlocks(ctx, dagSvc.Blocks())
}

// ProxyHTTPDownload proxies an http download request to the renterd node
func (n *Node) ProxyHTTPDownload(cid cid.Cid, r *http.Request, w http.ResponseWriter) error {
	block, err := n.store.GetBlock(r.Context(), cid)
	if err != nil {
		return err
	} else if block.Data.Offset != 0 {
		return errors.New("cannot proxy partial downloads")
	}

	target, err := url.Parse(n.renterd.Address + "/objects/" + cid.Hash().B58String())
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
func (n *Node) CalculateBlocks(ctx context.Context, r io.Reader) ([]Block, error) {
	dagSvc := NewUnixFileUploader("", io.Discard, io.Discard, n.log.Named("calculate"))

	spl := chunker.NewSizeSplitter(r, chunker.DefaultBlockSize)

	dbp := ihelpers.DagBuilderParams{
		Maxlinks: ihelpers.DefaultLinksPerBlock,
		Dagserv:  dagSvc,
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

// New creates a new Sia IPFS store
func New(store Store, cfg config.Renterd, log *zap.Logger) *Node {
	return &Node{
		store: store,
		log:   log,

		renterd: cfg,
	}
}
