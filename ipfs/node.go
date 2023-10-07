package ipfs

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	fsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	kdht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
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
	dht  *kdht.IpfsDHT
	host host.Host

	blockstore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	bitswap      *bitswap.Bitswap
}

// Close closes the node
func (n *Node) Close() error {
	n.dht.Close()
	n.bitswap.Close()
	n.host.Close()
	n.blockService.Close()
	return nil
}

// DownloadCID downloads the CID
func (n *Node) DownloadCID(ctx context.Context, c cid.Cid) (io.ReadSeekCloser, error) {
	dagSess := merkledag.NewSession(ctx, n.dagService)
	rootNode, err := dagSess.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get root node: %w", err)
	}

	dr, err := fsio.NewDagReader(ctx, rootNode, dagSess)
	return dr, err
}

// PeerID returns the peer ID of the node
func (n *Node) PeerID() peer.ID {
	return n.dht.PeerID()
}

// Peers returns the list of peers in the routing table
func (n *Node) Peers() []peer.ID {
	return n.dht.RoutingTable().ListPeers()
}

func mustParsePeer(s string) peer.AddrInfo {
	info, err := peer.AddrInfoFromString(s)
	if err != nil {
		panic(err)
	}
	return *info
}

// NewNode creates a new IPFS node
func NewNode(ctx context.Context, privateKey crypto.PrivKey, cfg config.IPFS, blockstore blockstore.Blockstore) (*Node, error) {
	cmgr, err := connmgr.NewConnManager(600, 900)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddresses...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
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

	dht, err := kdht.New(ctx, host,
		kdht.Mode(kdht.ModeServer),
		kdht.BootstrapPeers(bootstrapPeers...))
	if err != nil {
		return nil, fmt.Errorf("failed to create dht: %w", err)
	}

	bitswapNet := network.NewFromIpfsHost(host, dht)
	bitswap := bitswap.New(ctx, bitswapNet, blockstore)

	blockServ := blockservice.New(blockstore, bitswap)
	dagService := merkledag.NewDAGService(blockServ)

	return &Node{
		dht:          dht,
		host:         host,
		blockstore:   blockstore,
		bitswap:      bitswap,
		blockService: blockServ,
		dagService:   dagService,
	}, nil
}
