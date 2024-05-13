package config

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type (
	// Renterd contains the address, password, and bucket on the renterd worker
	Renterd struct {
		WorkerAddress  string `yaml:"workerAddress"`
		WorkerPassword string `yaml:"workerPassword"`
		BusAddress     string `yaml:"busAddress"`
		BusPassword    string `yaml:"busPassword"`
		Bucket         string `yaml:"bucket"`
	}

	// BlockStore configures the blockstore.
	BlockStore struct {
		// MaxConcurrent is the maximum number of concurrent block fetches.
		MaxConcurrent int `yaml:"maxConcurrent"`
		// CacheSize is the maximum number of blocks to cache in memory.
		CacheSize int `yaml:"cacheSize"`
	}

	// RemoteFetch contains settings for enabling/disabling remote IPFS block
	// fetching.
	RemoteFetch struct {
		// Enabled indicates whether remote IPFS block fetching is enabled.
		// If false, all served IPFS blocks must be pinned locally.
		Enabled bool `yaml:"enabled"`
		// Allowlist contains the CIDs of blocks that are allowed to be
		// fetched remotely. If empty, all blocks are allowed.
		AllowList []cid.Cid `yaml:"allowlist"`
	}

	// HTTPGateway contains the configuration for the IPFS HTTP gateway
	HTTPGateway struct {
		ListenAddress     string      `yaml:"listenAddress"`
		RedirectPathStyle bool        `yaml:"redirectPathStyle"`
		Fetch             RemoteFetch `yaml:"fetch"`
	}

	// IPFSPeer contains the configuration for additional IPFS peers
	IPFSPeer struct {
		ID        peer.ID  `yaml:"id"`
		Addresses []string `yaml:"addresses"`
	}

	// IPFS contains the configuration for the IPFS node
	IPFS struct {
		PrivateKey        string       `yaml:"privateKey"`
		ListenAddresses   []string     `yaml:"listenAddresses"`
		AnnounceAddresses []string     `yaml:"announceAddresses"`
		Peers             []IPFSPeer   `yaml:"peers"`
		Gateway           HTTPGateway  `yaml:"gateway"`
		Provider          IPFSProvider `yaml:"provider"`
	}

	// IPFSProvider contains the configuration for the IPFS provider
	IPFSProvider struct {
		BatchSize int           `yaml:"batchSize"`
		Interval  time.Duration `yaml:"interval"`
		Timeout   time.Duration `yaml:"timeout"`
	}

	// API contains the listen address of the API server
	API struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Log contains the log settings
	Log struct {
		Level string `yaml:"level"`
	}

	// Config contains the configuration for fsd
	Config struct {
		Renterd    Renterd    `yaml:"renterd"`
		BlockStore BlockStore `yaml:"blockstore"`
		IPFS       IPFS       `yaml:"ipfs"`
		API        API        `yaml:"api"`
		Log        Log        `yaml:"log"`
	}
)
