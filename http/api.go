package http

import (
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	apiServer struct {
		ipfs *ipfs.Node
		log  *zap.Logger
	}
)

func (as *apiServer) handleListPeers(jc jape.Context) {
	jc.Encode(as.ipfs.Peers())
}

func (as *apiServer) handleAddPeer(jc jape.Context) {
	var peer peer.AddrInfo
	if err := jc.Decode(&peer); err != nil {
		return
	}
	as.ipfs.AddPeer(peer)
}

func (as *apiServer) handleCARPin(jc jape.Context) {
	defer jc.Request.Body.Close()

	lr := io.LimitReader(jc.Request.Body, 256<<20) // 256MB
	if err := as.ipfs.PinCAR(jc.Request.Context(), lr); err != nil {
		jc.Error(err, http.StatusBadRequest)
	}
}

func (as *apiServer) handlePinCID(jc jape.Context) {
	var c cid.Cid
	var recursive bool
	if jc.DecodeParam("cid", &c) != nil {
		return
	} else if jc.DecodeForm("recursive", &recursive) != nil {
		return
	}

	if err := as.ipfs.Pin(jc.Request.Context(), c, recursive); err != nil {
		jc.Error(err, http.StatusBadRequest)
	}
}

// NewAPIHandler returns a new http.Handler that handles requests to the api
func NewAPIHandler(ipfs *ipfs.Node, cfg config.Config, log *zap.Logger) http.Handler {
	s := &apiServer{
		ipfs: ipfs,
		log:  log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/unixfs/upload": s.handleUnixFSUpload,
		"PUT /api/car/pin":        s.handleCARPin,
		"PUT /api/cid/:cid/pin":   s.handlePinCID,
		"GET /api/peers":          s.handleListPeers,
		"PUT /api/peers":          s.handleAddPeer,
	})
}
