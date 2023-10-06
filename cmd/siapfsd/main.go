package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siapfs/build"
	"go.sia.tech/siapfs/config"
	shttp "go.sia.tech/siapfs/http"
	"go.sia.tech/siapfs/persist/badger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

var (
	dir = "."
	cfg = config.Config{
		Renterd: config.Renterd{
			Address:  "http://localhost:9980/api/worker",
			Password: "password",
			Bucket:   "ipfs",
		},
		IPFS: config.IPFS{
			GatewayAddress: ":8080",
		},
		API: config.API{
			Address: ":8081",
		},
	}
)

// mustLoadConfig loads the config file specified by the HOSTD_CONFIG_PATH. If
// the config file does not exist, it will not be loaded.
func mustLoadConfig(dir string, log *zap.Logger) {
	configPath := filepath.Join(dir, "siapfsd.yml")

	// If the config file doesn't exist, don't try to load it.
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return
	}

	f, err := os.Open(configPath)
	if err != nil {
		log.Fatal("failed to open config file", zap.Error(err))
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		log.Fatal("failed to decode config file", zap.Error(err))
	}
}

func main() {
	// configure console logging note: this is configured before anything else
	// to have consistent logging. File logging will be added after the cli
	// flags and config is parsed
	consoleCfg := zap.NewProductionEncoderConfig()
	consoleCfg.TimeKey = "" // prevent duplicate timestamps
	consoleCfg.EncodeTime = zapcore.RFC3339TimeEncoder
	consoleCfg.EncodeDuration = zapcore.StringDurationEncoder
	consoleCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleCfg.StacktraceKey = ""
	consoleCfg.CallerKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(consoleCfg)

	// only log info messages to console unless stdout logging is enabled
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), zap.NewAtomicLevelAt(zap.InfoLevel))
	log := zap.New(consoleCore, zap.AddCaller())
	defer log.Sync()
	// redirect stdlib log to zap
	zap.RedirectStdLog(log.Named("stdlib"))

	flag.StringVar(&dir, "dir", dir, "directory to use for data")
	flag.Parse()

	mustLoadConfig(dir, log)

	ipfsPath := filepath.Join(dir, "ipfs")

	if err := setupPlugins(filepath.Join(ipfsPath, "plugins")); err != nil {
		log.Fatal("failed to setup ipfs plugins", zap.Error(err))
	}

	if flag.Arg(0) == "init" {
		log.Info("initializing repo", zap.String("path", ipfsPath))
		if err := initRepo(ipfsPath); err != nil {
			log.Fatal("failed to init repo", zap.Error(err))
		}
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	ds, err := badger.OpenDatabase(filepath.Join(dir, "siapfsd.badgerdb"), log.Named("badger"))
	if err != nil {
		log.Fatal("failed to open badger database", zap.Error(err))
	}
	defer ds.Close()

	client := worker.NewClient(cfg.Renterd.Address, cfg.Renterd.Password)
	coreAPI, node, err := createNode(ctx, ipfsPath, ds, client, cfg.Renterd.Bucket)
	if err != nil {
		log.Fatal("failed to start ipfs node", zap.Error(err))
	}
	defer node.Close()

	apiListener, err := net.Listen("tcp", cfg.API.Address)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer apiListener.Close()

	gatewayListener, err := net.Listen("tcp", cfg.IPFS.GatewayAddress)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer gatewayListener.Close()

	apiServer := &http.Server{
		Handler: jape.BasicAuth(cfg.API.Password)(shttp.NewAPIHandler(cfg.Renterd, ds, log.Named("api"))),
	}
	defer apiServer.Close()

	gatewayServer := &http.Server{
		Handler: jape.BasicAuth(cfg.API.Password)(shttp.NewIPFSHandler(cfg.Renterd, coreAPI, ds, log.Named("gateway"))),
	}
	defer gatewayServer.Close()

	go func() {
		if err := apiServer.Serve(apiListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("failed to serve api", zap.Error(err))
		}
	}()

	go func() {
		if err := gatewayServer.Serve(gatewayListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("failed to serve gateway", zap.Error(err))
		}
	}()

	log.Info("siapfsd started",
		zap.String("apiAddress", apiListener.Addr().String()),
		zap.String("gatewayAddress", gatewayListener.Addr().String()),
		zap.String("version", build.Version()),
		zap.String("revision", build.Commit()),
		zap.Time("buildTime", build.Time()))

	<-ctx.Done()
}
