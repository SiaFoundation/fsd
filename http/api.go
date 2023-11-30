package http

import (
	"bufio"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/sia"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	apiServer struct {
		ipfs *ipfs.Node
		sia  *sia.Node
		log  *zap.Logger
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
	// not properly handle anything but standard unixfs files with the default
	// block size
	rr, err := as.ipfs.DownloadCID(ctx, c, nil)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	defer rr.Close()

	var opts sia.CIDOptions
	switch c.Version() {
	case 1:
		prefix := c.Prefix()
		opts.CIDBuilder = cid.V1Builder{Codec: prefix.Codec, MhType: prefix.MhType, MhLength: prefix.MhLength}
	case 0:
		opts.CIDBuilder = cid.V0Builder{}
	}

	br := bufio.NewReaderSize(rr, 256<<20) // 256 MiB
	c, err = as.sia.UploadFile(jc.Request.Context(), br, opts)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// return the calculated cid
	jc.Encode(c.String())
}

func (as *apiServer) handleUnixFSUpload(jc jape.Context) {
	body := jc.Request.Body
	defer body.Close()

	var cidVersion int
	if err := jc.DecodeForm("cidVersion", &cidVersion); err != nil {
		return
	}

	var opts sia.CIDOptions
	switch cidVersion {
	case 0:
		opts.CIDBuilder = cid.V0Builder{}
	case 1:
		opts.CIDBuilder = cid.V1Builder{Codec: uint64(multicodec.DagPb), MhType: multihash.SHA2_256}
	}

	if err := jc.DecodeForm("rawLeaves", &opts.RawLeaves); err != nil {
		return
	} else if err := jc.DecodeForm("maxLinks", &opts.MaxLinks); err != nil {
		return
	} else if err := jc.DecodeForm("blockSize", &opts.BlockSize); err != nil {
		return
	}

	br := bufio.NewReaderSize(body, 256<<20) // 256 MiB
	c, err := as.sia.UploadFile(jc.Request.Context(), br, opts)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// return the calculated cid
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
		ipfs: ipfs,
		sia:  sia,
		log:  log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/cid/calculate":   s.handleCalculate,
		"POST /api/cid/verify/:cid": s.handleVerifyCID,
		"POST /api/unixfs/upload":   s.handleUnixFSUpload,
		"POST /api/pin/:cid":        s.handlePin,
	})
}
