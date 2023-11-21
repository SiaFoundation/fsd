package http

import (
	"errors"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
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

	opts := sia.CIDOptions{
		CIDBuilder: cid.V1Builder{Codec: uint64(multicodec.DagPb), MhType: multihash.SHA2_256},
		RawLeaves:  true,
	}

	if err := jc.DecodeForm("rawLeaves", &opts.RawLeaves); err != nil {
		return
	} else if err := jc.DecodeForm("maxLinks", &opts.MaxLinks); err != nil {
		return
	} else if err := jc.DecodeForm("blockSize", &opts.BlockSize); err != nil {
		return
	}

	blocks, err := as.sia.CalculateBlocks(ctx, body, opts)
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
	c, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	// TODO: break this out for better support, the current implementation will
	// not handle anything but standard unixfs files with the default block size
	r, err := as.ipfs.DownloadCID(ctx, c, nil)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	defer r.Close()

	var opts sia.CIDOptions
	switch c.Version() {
	case 1:
		prefix := c.Prefix()
		opts.CIDBuilder = cid.V1Builder{Codec: prefix.Codec, MhType: prefix.MhType, MhLength: prefix.MhLength}
		opts.RawLeaves = true
	case 0:
		opts.CIDBuilder = cid.V0Builder{}
	}

	if err := as.sia.UploadCID(ctx, c, r, opts); err != nil {
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
	c, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if c.Version() != 1 {
		jc.Error(errors.New("only v1 CIDs are supported"), http.StatusBadRequest)
		return
	}

	body := jc.Request.Body
	defer body.Close()

	prefix := c.Prefix()
	opts := sia.CIDOptions{
		CIDBuilder: cid.V1Builder{Codec: prefix.Codec, MhType: prefix.MhType, MhLength: prefix.MhLength},
		RawLeaves:  true,
	}

	if err := jc.DecodeForm("rawLeaves", &opts.RawLeaves); err != nil {
		return
	} else if err := jc.DecodeForm("maxLinks", &opts.MaxLinks); err != nil {
		return
	} else if err := jc.DecodeForm("blockSize", &opts.BlockSize); err != nil {
		return
	}

	err = as.sia.UploadCID(ctx, c, body, opts)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// the root cid is the first block
	jc.Encode(c.String())
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
