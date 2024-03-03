package http

import (
	"net/http"

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

// NewAPIHandler returns a new http.Handler that handles requests to the api
func NewAPIHandler(ipfs *ipfs.Node, cfg config.Config, log *zap.Logger) http.Handler {
	s := &apiServer{
		ipfs: ipfs,
		log:  log,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/unixfs/upload": s.handleUnixFSUpload,
		// "POST /api/pin/:cid":         s.handlePin,
		"GET /api/peers": s.handleListPeers,
		"PUT /api/peers": s.handleAddPeer,
	})
}
