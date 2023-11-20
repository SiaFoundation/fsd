package http

import (
	"net/http"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/sia"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

type (
	apiServer struct {
		ipfs   *ipfs.Node
		sia    *sia.Node
		worker *worker.Client
		log    *zap.Logger

		renterd config.Renterd
	}
)

func (as *apiServer) handleCalculate(jc jape.Context) {
	ctx := jc.Request.Context()

	body := jc.Request.Body
	defer body.Close()

	blocks, err := as.sia.CalculateBlocks(ctx, body)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(blocks)
}

func (as *apiServer) handlePin(jc jape.Context) {
	ctx := jc.Request.Context()
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}
	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	r, err := as.ipfs.DownloadCID(ctx, cid, nil)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	defer r.Close()

	if err := as.sia.UploadCID(ctx, cid, r); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (as *apiServer) handleUpload(jc jape.Context) {
	ctx := jc.Request.Context()
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
	defer body.Close()

	err = as.sia.UploadCID(ctx, cid, body)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// the root cid is the first block
	jc.Encode(cid.String())
}

func (as *apiServer) handleVerifyCID(jc jape.Context) {
	ctx := jc.Request.Context()
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}
	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	if err := as.sia.VerifyCID(ctx, cid); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

// NewAPIHandler returns a new http.Handler that handles requests to the api
func NewAPIHandler(ipfs *ipfs.Node, sia *sia.Node, cfg config.Config, log *zap.Logger) http.Handler {
	s := &apiServer{
		worker:  worker.NewClient(cfg.Renterd.Address, cfg.Renterd.Password),
		renterd: cfg.Renterd,

		ipfs: ipfs,
		sia:  sia,
		log:  log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/cid/calculate":   s.handleCalculate,
		"POST /api/cid/verify/:cid": s.handleVerifyCID,
		"POST /api/upload/:cid":     s.handleUpload,
		"POST /api/pin/:cid":        s.handlePin,
	})
}
