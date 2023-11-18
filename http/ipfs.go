package http

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/sia"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type ipfsGatewayServer struct {
	ipfs *ipfs.Node
	sia  *sia.Node
	log  *zap.Logger

	config config.Config
}

func (is *ipfsGatewayServer) handleIPFS(jc jape.Context) {
	ctx := jc.Request.Context()

	var pathStr string
	if err := jc.DecodeParam("path", &pathStr); err != nil {
		return
	}

	var cidStr string
	is.log.Debug("downloading file", zap.String("path", pathStr))
	pathStr = strings.TrimPrefix(pathStr, "/") // remove leading slash
	path := strings.Split(pathStr, "/")
	if len(path) == 0 || path[0] == "" {
		jc.Error(errors.New("bad path"), http.StatusBadRequest)
		return
	} else if len(path) == 1 {
		cidStr = path[0]
	} else {
		cidStr = path[0]
		path = path[1:]
	}

	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	err = is.sia.ProxyHTTPDownload(cid, jc.Request, jc.ResponseWriter)
	if errors.Is(err, sia.ErrNotFound) && is.config.IPFS.FetchRemote {
		is.log.Info("downloading from ipfs", zap.String("cid", cid.Hash().B58String()))
		r, err := is.ipfs.DownloadCID(ctx, cid, path)
		if err != nil {
			jc.Error(err, http.StatusInternalServerError)
			is.log.Error("failed to download cid", zap.Error(err))
			return
		}
		defer r.Close()

		io.Copy(jc.ResponseWriter, r)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		is.log.Error("failed to get block", zap.Error(err))
		return
	}
}

// NewIPFSGatewayHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSGatewayHandler(ipfs *ipfs.Node, sia *sia.Node, cfg config.Config, log *zap.Logger) http.Handler {
	s := &ipfsGatewayServer{
		ipfs: ipfs,
		sia:  sia,
		log:  log,

		config: cfg,
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /ipfs/*path": s.handleIPFS,
	})
}
