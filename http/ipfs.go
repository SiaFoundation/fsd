package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	"go.sia.tech/jape"
	"go.sia.tech/siapfs/config"
	"go.sia.tech/siapfs/ipfs"
	"go.uber.org/zap"
)

type ipfsServer struct {
	renterd config.Renterd
	ipfs    iface.CoreAPI

	store Store
	log   *zap.Logger
}

func (is *ipfsServer) handleIPFS(jc jape.Context) {
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}

	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	block, err := is.store.GetBlock(cid)
	if errors.Is(ipfs.ErrNotFound, err) {
		node, err := is.ipfs.Unixfs().Get(jc.Request.Context(), path.IpfsPath(cid))
		if err != nil {
			jc.Error(err, http.StatusInternalServerError)
			return
		}
		switch node := node.(type) {
		case files.File:
			jc.ResponseWriter.WriteHeader(http.StatusOK)
			if _, err := io.Copy(jc.ResponseWriter, node); err != nil {
				is.log.Error("failed to copy file", zap.Error(err))
				return
			}
		default:
			jc.Error(fmt.Errorf("unsupported %T", node), http.StatusBadRequest)
		}
		return
	}

	is.log.Info("downloading block from renterd", zap.String("cid", cid.Hash().B58String()), zap.String("key", block.Key), zap.Uint64("offset", block.Offset), zap.Uint64("length", block.Length))
	// note: download object from bucket is broken
	reader, err := downloadObject(jc.Request.Context(), is.renterd, block.Key, block.Offset, block.Length)
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
func NewIPFSHandler(renterd config.Renterd, node iface.CoreAPI, ds Store, log *zap.Logger) http.Handler {
	s := &ipfsServer{
		renterd: renterd,
		ipfs:    node,
		store:   ds,
		log:     log,
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /ipfs/:cid": s.handleIPFS,
	})
}