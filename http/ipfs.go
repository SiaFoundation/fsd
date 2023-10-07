package http

import (
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type ipfsServer struct {
	renterd config.Renterd

	store Store
	log   *zap.Logger
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
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		is.log.Error("failed to get block", zap.Error(err))
		return
	}

	is.log.Info("downloading block from renterd", zap.String("cid", cid.Hash().B58String()), zap.String("key", block.Key), zap.Uint64("offset", block.Offset), zap.Uint64("length", block.Length))
	// note: download object from bucket is broken
	reader, err := downloadObject(ctx, is.renterd, block.Key, block.Offset, block.Length)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		is.log.Error("failed to download object", zap.Error(err))
		return
	}
	defer reader.Close()

	jc.ResponseWriter.WriteHeader(http.StatusOK)
	if _, err := io.Copy(jc.ResponseWriter, reader); err != nil {
		is.log.Error("failed to copy file", zap.Error(err))
		return
	}
}

// NewIPFSHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSHandler(renterd config.Renterd, ds Store, log *zap.Logger) http.Handler {
	s := &ipfsServer{
		renterd: renterd,
		store:   ds,
		log:     log,
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /ipfs/:cid": s.handleIPFS,
	})
}
