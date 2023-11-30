package ipfs

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ipfs/boxo/bitswap"
	bnetwork "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	fsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/provider"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"go.sia.tech/fsd/config"
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
	host host.Host
	frt  *fullrt.FullRT

	blockstore   blockstore.Blockstore
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
func (n *Node) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return n.blockService.GetBlock(ctx, c)
}

// DownloadCID downloads a CID from IPFS
func (n *Node) DownloadCID(ctx context.Context, c cid.Cid, path []string) (io.ReadSeekCloser, error) {
	dagSess := merkledag.NewSession(ctx, n.dagService)
	rootNode, err := dagSess.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get root node: %w", err)
	}

	var traverse func(context.Context, format.Node, []string) (format.Node, error)
	traverse = func(ctx context.Context, parent format.Node, path []string) (format.Node, error) {
		if len(path) == 0 {
			return parent, nil
		}

		childLink, rem, err := parent.Resolve(path)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve path %q: %w", strings.Join(path, "/"), err)
		}

		switch v := childLink.(type) {
		case *format.Link:
			childNode, err := dagSess.Get(ctx, v.Cid)
			if err != nil {
				return nil, fmt.Errorf("failed to get child node %q: %w", v.Cid, err)
			}
			return traverse(ctx, childNode, rem)
		default:
			return nil, fmt.Errorf("expected link node, got %T", childLink)
		}
	}

	node, err := traverse(ctx, rootNode, path)
	if err != nil {
		return nil, fmt.Errorf("failed to traverse path: %w", err)
	}

	dr, err := fsio.NewDagReader(ctx, node, dagSess)
	return dr, err
}

// PeerID returns the peer ID of the node
func (n *Node) PeerID() peer.ID {
	return n.frt.Host().ID()
}

// Peers returns the list of peers in the routing table
func (n *Node) Peers() []peer.ID {
	return n.frt.Host().Network().Peers()
}

func mustParsePeer(s string) peer.AddrInfo {
	info, err := peer.AddrInfoFromString(s)
	if err != nil {
		panic(err)
	}
	return *info
}

// Provide broadcasts a CID to the network
func (n *Node) Provide(c cid.Cid) error {
	return n.provider.Provide(c)
}

// NewNode creates a new IPFS node
func NewNode(ctx context.Context, privateKey crypto.PrivKey, cfg config.IPFS, ds datastore.Batching, bs blockstore.Blockstore) (*Node, error) {
	cmgr, err := connmgr.NewConnManager(600, 900)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddresses...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ResourceManager(new(network.NullResourceManager)),
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
		dht.Datastore(ds),
	}
	frt, err := fullrt.NewFullRT(host, dht.DefaultPrefix, fullrt.DHTOption(dhtOpts...))
	if err != nil {
		return nil, fmt.Errorf("failed to create fullrt: %w", err)
	}

	bitswapNet := bnetwork.NewFromIpfsHost(host, frt)
	bitswap := bitswap.New(ctx, bitswapNet, bs)

	blockServ := blockservice.New(bs, bitswap)
	dagService := merkledag.NewDAGService(blockServ)
	bsp := provider.NewBlockstoreProvider(bs)

	prov, err := provider.New(ds, provider.KeyProvider(bsp), provider.Online(frt))
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	return &Node{
		frt:          frt,
		host:         host,
		blockstore:   bs,
		bitswap:      bitswap,
		blockService: blockServ,
		dagService:   dagService,
		provider:     prov,
	}, nil
}
