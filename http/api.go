package http

import (
	"errors"
	"io"
	"net/http"

	blocks "github.com/ipfs/go-block-format"
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

// handleBlocksCIDPUT handles requests to pin a block by CID
func (as *apiServer) handleBlocksCIDPUT(jc jape.Context) {
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

// handleBlocksPUT handles requests to upload a raw block
func (as *apiServer) handleBlocksPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	defer jc.Request.Body.Close()

	lr := io.LimitReader(jc.Request.Body, 4<<20) // 4MB
	buf, err := io.ReadAll(lr)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		jc.Error(err, http.StatusRequestEntityTooLarge)
		return
	} else if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	block := blocks.NewBlock(buf)

	ok, err := as.ipfs.HasBlock(ctx, block.Cid())
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if ok {
		jc.Encode(block.Cid())
		return
	} else if err := as.ipfs.AddBlock(ctx, block); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(block.Cid())
}

func (as *apiServer) handleProviderStats(jc jape.Context) {
	stats, err := as.ipfs.ReproviderStats()
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(ProviderStatsResp{
		TotalProvides:          stats.TotalProvides,
		LastReprovideBatchSize: stats.LastReprovideBatchSize,
		AvgProvideDuration:     stats.AvgProvideDuration,
		LastReprovideDuration:  stats.LastReprovideDuration,
	})
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
		"PUT /api/blocks":         s.handleBlocksPUT,
		"PUT /api/blocks/:cid":    s.handleBlocksCIDPUT,
		"GET /api/peers":          s.handleListPeers,
		"PUT /api/peers":          s.handleAddPeer,
		"GET /api/provider/stats": s.handleProviderStats,
	})
}
