package http

import (
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type ipfsServer struct {
	store Store
	node  *ipfs.Node
	log   *zap.Logger

	ipfs    config.IPFS
	renterd config.Renterd
}

func (is *ipfsServer) handleIPFS(jc jape.Context) {
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

	block, err := is.store.GetBlock(ctx, cid)
	if format.IsNotFound(err) && is.ipfs.FetchRemote {
		is.log.Info("downloading from ipfs", zap.String("cid", cid.Hash().B58String()))
		r, err := is.node.DownloadCID(ctx, cid)
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

	is.log.Info("downloading from renterd", zap.String("cid", cid.Hash().B58String()), zap.String("key", block.Key), zap.Uint64("offset", block.Offset), zap.Uint64("length", block.Length))
	reader, err := downloadObject(ctx, is.renterd, block.Key, block.Offset, block.Length)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		is.log.Error("failed to download object", zap.Error(err))
		return
	}
	defer reader.Close()

	if _, err := io.Copy(jc.ResponseWriter, reader); err != nil {
		is.log.Error("failed to copy file", zap.Error(err))
		return
	}
}

// NewIPFSHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSHandler(node *ipfs.Node, store Store, cfg config.Config, log *zap.Logger) http.Handler {
	s := &ipfsServer{
		node:  node,
		store: store,
		log:   log,

		ipfs:    cfg.IPFS,
		renterd: cfg.Renterd,
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /ipfs/:cid": s.handleIPFS,
	})
}
