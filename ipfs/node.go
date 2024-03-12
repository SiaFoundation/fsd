package ipfs

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bnetwork "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"go.sia.tech/fsd/config"
	"go.uber.org/zap"
)

var bootstrapPeers = []peer.AddrInfo{
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"),
	mustParsePeer("/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
	mustParsePeer("/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
}

// A Node is a minimal IPFS node
type Node struct {
	log  *zap.Logger
	host host.Host
	frt  *fullrt.FullRT

	blockService blockservice.BlockService
	dagService   format.DAGService
	bitswap      *bitswap.Bitswap
	provider     provider.System
}

// Close closes the node
func (n *Node) Close() error {
	n.frt.Close()
	n.bitswap.Close()
	n.provider.Close()
	n.host.Close()
	n.blockService.Close()
	return nil
}

// GetBlock fetches a block from the IPFS network
func (n *Node) GetBlock(ctx context.Context, c cid.Cid) (format.Node, error) {
	return n.dagService.Get(ctx, c)
}

// HasBlock checks if a block is locally pinned
func (n *Node) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	return n.blockService.Blockstore().Has(ctx, c)
}

// PeerID returns the peer ID of the node
func (n *Node) PeerID() peer.ID {
	return n.frt.Host().ID()
}

// Provide broadcasts a CID to the network
func (n *Node) Provide(c cid.Cid) error {
	return n.provider.Provide(c)
}

// Peers returns the list of peers in the routing table
func (n *Node) Peers() []peer.ID {
	return n.host.Peerstore().Peers()
}

// AddPeer adds a peer to the peerstore
func (n *Node) AddPeer(addr peer.AddrInfo) {
	n.host.Peerstore().AddAddrs(addr.ID, addr.Addrs, peerstore.AddressTTL)
}

// Pin pins a CID
func (n *Node) Pin(ctx context.Context, root cid.Cid, recursive bool) error {
	log := n.log.Named("Pin").With(zap.Stringer("rootCID", root), zap.Bool("recursive", recursive))
	if !recursive {
		block, err := n.dagService.Get(ctx, root)
		if err != nil {
			return fmt.Errorf("failed to get block: %w", err)
		} else if err := n.blockService.AddBlock(ctx, block); err != nil {
			return fmt.Errorf("failed to add block: %w", err)
		}
		return n.provider.Provide(root)
	}

	sess := merkledag.NewSession(ctx, n.dagService)
	seen := make(map[string]bool)
	err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(sess), root, func(c cid.Cid) bool {
		var key string
		switch c.Version() {
		case 0:
			key = cid.NewCidV1(c.Type(), c.Hash()).String()
		case 1:
			key = c.String()
		}
		if seen[key] {
			return false
		}
		log := log.With(zap.Stringer("childCID", c))
		log.Debug("pinning child")
		// TODO: queue and handle these correctly
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		node, err := sess.Get(ctx, c)
		if err != nil {
			log.Error("failed to get node", zap.Error(err))
			return false
		} else if err := n.blockService.AddBlock(ctx, node); err != nil {
			log.Error("failed to add block", zap.Error(err))
			return false
		}
		seen[key] = true
		log.Debug("pinned block")
		return true
	}, merkledag.Concurrent(), merkledag.IgnoreErrors())
	if err != nil {
		return fmt.Errorf("failed to walk DAG: %w", err)
	}
	return n.provider.Provide(root)
}

// PinCAR pins all blocks in a CAR file to the node. The input reader
// must be a valid CARv1 or CARv2 file.
func (n *Node) PinCAR(ctx context.Context, r io.Reader) error {
	log := n.log.Named("PinCAR")
	cr, err := car.NewBlockReader(r)
	if err != nil {
		return fmt.Errorf("failed to create blockstore: %w", err)
	}

	for block, err := cr.Next(); err != io.EOF; block, err = cr.Next() {
		if n.blockService.AddBlock(ctx, block); err != nil {
			return fmt.Errorf("failed to add block %q: %w", block.Cid(), err)
		}
		log.Debug("added block", zap.Stringer("cid", block.Cid()))
	}
	return nil
}

func mustParsePeer(s string) peer.AddrInfo {
	info, err := peer.AddrInfoFromString(s)
	if err != nil {
		panic(err)
	}
	return *info
}

// NewNode creates a new IPFS node
func NewNode(ctx context.Context, privateKey crypto.PrivKey, cfg config.IPFS, ds datastore.Batching, bs blockstore.Blockstore, log *zap.Logger) (*Node, error) {
	cmgr, err := connmgr.NewConnManager(600, 900)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)
	rm, err := rcmgr.NewResourceManager(limiter, rcmgr.WithMetricsDisabled())
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddresses...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ResourceManager(rm),
		libp2p.DefaultPeerstore,
		libp2p.DefaultTransports,
	}

	if len(cfg.AnnounceAddresses) != 0 {
		var addrs []multiaddr.Multiaddr
		for _, as := range cfg.AnnounceAddresses {
			addr, err := multiaddr.NewMultiaddr(as)
			if err != nil {
				return nil, fmt.Errorf("failed to parse announce address %q: %w", as, err)
			}
			addrs = append(addrs, addr)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrs
		}))
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.BootstrapPeers(bootstrapPeers...),
		dht.BucketSize(20),
		dht.Concurrency(30),
		dht.Datastore(ds),
	}
	frt, err := fullrt.NewFullRT(host, dht.DefaultPrefix, fullrt.DHTOption(dhtOpts...))
	if err != nil {
		return nil, fmt.Errorf("failed to create fullrt: %w", err)
	}

	bitswapOpts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(int(5 << 20)),
		bitswap.ProvideEnabled(true),
	}

	bitswapNet := bnetwork.NewFromIpfsHost(host, frt)
	bitswap := bitswap.New(ctx, bitswapNet, bs, bitswapOpts...)

	blockServ := blockservice.New(bs, bitswap)
	dagService := merkledag.NewDAGService(blockServ)

	providerOpts := []provider.Option{
		provider.KeyProvider(provider.NewBlockstoreProvider(bs)),
		provider.Online(frt),
		provider.ReproviderInterval(18 * time.Hour),
	}

	prov, err := provider.New(ds, providerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	for _, p := range cfg.Peers {
		mh := make([]multiaddr.Multiaddr, 0, len(p.Addresses))
		for _, addr := range p.Addresses {
			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse multiaddr %q: %w", addr, err)
			}
			mh = append(mh, maddr)
		}

		host.Peerstore().AddAddrs(p.ID, mh, peerstore.PermanentAddrTTL)
	}

	return &Node{
		log:          log,
		frt:          frt,
		host:         host,
		bitswap:      bitswap,
		blockService: blockServ,
		dagService:   dagService,
		provider:     prov,
	}, nil
}
