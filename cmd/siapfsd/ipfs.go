package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siapfs/ipfs/blockstore"
	"go.sia.tech/siapfs/persist/badger"
)

func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

func initRepo(repoPath string) error {
	identity, err := config.CreateIdentity(os.Stdout, []options.KeyGenerateOption{
		options.Key.Type(options.Ed25519Key),
	})
	if err != nil {
		return fmt.Errorf("failed to create identity: %w", err)
	}
	conf, err := config.InitWithIdentity(identity)
	if err != nil {
		return fmt.Errorf("failed to init config: %w", err)
	}
	return fsrepo.Init(repoPath, conf)
}

func createNode(ctx context.Context, repoPath string, db *badger.Store, renterd *worker.Client, bucket string) (iface.CoreAPI, *core.IpfsNode, error) {
	r, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open repo: %w", err)
	}
	defer r.Close()

	node, err := core.NewNode(ctx, &core.BuildCfg{
		Online: true,
		Repo:   r,
		ExtraOpts: map[string]bool{
			"mplex": true,
		},
		Permanent: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create node: %w", err)
	}

	node.Blockstore = blockstore.New(bucket, db, cfg.Renterd)
	coreAPI, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create coreapi: %w", err)
	}
	return coreAPI, node, nil
}
