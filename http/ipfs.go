package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/sia"
	"go.uber.org/zap"
)

type ipfsGatewayServer struct {
	ipfs *ipfs.Node
	sia  *sia.Node
	log  *zap.Logger

	config config.Config
}

func (is *ipfsGatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var cidStr string
	var path []string

	is.log.Debug("request", zap.String("host", r.Host), zap.String("path", r.URL.Path))

	if strings.HasPrefix(r.URL.Path, "/ipfs/") {
		path = strings.Split(r.URL.Path, "/")
		cidStr, path = path[2], path[3:]
	} else {
		hostParts := strings.Split(r.Host, ".") // try to parse the subdomain as a CID
		cidStr = hostParts[0]
		path = strings.Split(r.URL.Path, "/")
	}

	cid, err := cid.Parse(cidStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse cid %q", cidStr), http.StatusBadRequest)
		return
	}

	is.log.Info("serving", zap.String("cid", cid.Hash().B58String()), zap.String("path", r.URL.Path))

	// TODO: support paths in Sia proxied downloads
	err = is.sia.ProxyHTTPDownload(cid, r, w)
	if errors.Is(err, sia.ErrNotFound) && is.config.IPFS.FetchRemote {
		is.log.Info("downloading from ipfs", zap.String("cid", cid.Hash().B58String()))
		r, err := is.ipfs.DownloadCID(ctx, cid, path)
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			is.log.Error("failed to download cid", zap.Error(err))
			return
		}
		defer r.Close()

		io.Copy(w, r)
	} else if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		is.log.Error("failed to get block", zap.Error(err))
	}
}

// NewIPFSGatewayHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSGatewayHandler(ipfs *ipfs.Node, sia *sia.Node, cfg config.Config, log *zap.Logger) http.Handler {
	return &ipfsGatewayServer{
		ipfs: ipfs,
		sia:  sia,
		log:  log,

		config: cfg,
	}
}
