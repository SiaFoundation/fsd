package http

import (
	"bufio"
	"net/http"

	"github.com/ipfs/go-cid"
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

	var opts sia.UnixFSOptions
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

// NewAPIHandler returns a new http.Handler that handles requests to the api
func NewAPIHandler(ipfs *ipfs.Node, sia *sia.Node, cfg config.Config, log *zap.Logger) http.Handler {
	s := &apiServer{
		ipfs: ipfs,
		sia:  sia,
		log:  log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/unixfs/calculate": s.handleUnixFSCalculate,
		"POST /api/unixfs/upload":    s.handleUnixFSUpload,
		"POST /api/pin/:cid":         s.handlePin,
	})
}
