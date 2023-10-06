package http

import (
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

type (
	// A Store is a persistent store for IPFS blocks
	Store interface {
		GetBlock(cid.Cid) (ipfs.Block, error)
		AddBlocks(blocks []ipfs.Block) error
	}

	apiServer struct {
		worker  *worker.Client
		renterd config.Renterd

		store Store
		log   *zap.Logger
	}
)

func (as *apiServer) handleCalculate(jc jape.Context) {
	body := jc.Request.Body
	defer body.Close()

	blocks, err := ipfs.BuildBalancedCID("test", body)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(blocks)
}

func (as *apiServer) handleUpload(jc jape.Context) {
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}
	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	body := jc.Request.Body

	pr, pw := io.Pipe()
	r := io.TeeReader(body, pw)
	uploadErr := make(chan error, 1)

	go func() {
		defer pw.Close()
		defer body.Close()
		defer close(uploadErr)

		_, err = as.worker.UploadObject(jc.Request.Context(), r, cid.Hash().B58String(), api.UploadWithBucket(as.renterd.Bucket))
		if err != nil {
			uploadErr <- err
		}
	}()

	blocks, err := ipfs.BuildBalancedCID(cidStr, pr)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	if err := <-uploadErr; err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	if err := as.store.AddBlocks(blocks); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	// the root cid is the first block
	rootCID := blocks[0].CID
	jc.Encode(rootCID.Hash().B58String())
	as.log.Info("uploaded cid", zap.String("rootCID", rootCID.Hash().B58String()), zap.Int("blocks", len(blocks)))
}

// NewAPIHandler returns a new http.Handler that handles requests to the api
func NewAPIHandler(renterd config.Renterd, ds Store, log *zap.Logger) http.Handler {
	s := &apiServer{
		worker:  worker.NewClient(renterd.Address, renterd.Password),
		renterd: renterd,

		store: ds,
		log:   log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/cid/calculate": s.handleCalculate,
		"POST /api/upload/:cid":   s.handleUpload,
	})
}
