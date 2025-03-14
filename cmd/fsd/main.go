package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	levelds "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"go.sia.tech/fsd/build"
	"go.sia.tech/fsd/config"
	shttp "go.sia.tech/fsd/http"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/persist/sqlite"
	"go.sia.tech/fsd/renterd"
	"go.sia.tech/fsd/renterd/downloader"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/v2/bus"
	"go.sia.tech/renterd/v2/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"lukechampine.com/frand"

	_ "net/http/pprof"
)

var (
	dir = "."
	cfg = config.Config{
		Renterd: config.Renterd{
			WorkerAddress: "http://localhost:9980/api/worker",
			BusAddress:    "http://localhost:9980/api/bus",
			Bucket:        "ipfs",
		},
		BlockStore: config.BlockStore{
			CacheSize:     1024, // 1024 blocks = max of 4GiB
			MaxConcurrent: 1000,
		},
		IPFS: config.IPFS{
			Gateway: config.HTTPGateway{
				ListenAddress: ":8080",
			},
			Provider: config.IPFSProvider{
				BatchSize: 5000,
				Interval:  18 * time.Hour,
				Timeout:   30 * time.Minute,
			},
		},
		API: config.API{
			Address: ":8081",
		},
		Log: config.Log{
			Level: "info",
		},
	}
)

// mustLoadConfig loads the config file.
func mustLoadConfig(dir string, log *zap.Logger) {
	configPath := filepath.Join(dir, "fsd.yml")

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
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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

	var level zap.AtomicLevel
	switch cfg.Log.Level {
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		log.Fatal("invalid log level", zap.String("level", cfg.Log.Level))
	}

	log = log.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)
	}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var privateKey crypto.PrivKey
	var err error
	if cfg.IPFS.PrivateKey != "" {
		buf, err := hex.DecodeString(strings.TrimPrefix(cfg.IPFS.PrivateKey, "ed25519:"))
		if err != nil {
			log.Fatal("failed to decode private key", zap.Error(err))
		} else if len(buf) != 64 {
			log.Fatal("private key must be 64 bytes")
		}
		privateKey, err = crypto.UnmarshalEd25519PrivateKey(buf)
		if err != nil {
			log.Fatal("failed to unmarshal private key", zap.Error(err))
		}
	} else {
		privateKey, _, err = crypto.GenerateEd25519Key(frand.Reader)
		if err != nil {
			log.Fatal("failed to generate private key", zap.Error(err))
		}
	}

	ds, err := levelds.NewDatastore(filepath.Join(dir, "fsdds.leveldb"), nil)
	if err != nil {
		log.Fatal("failed to open leveldb datastore", zap.Error(err))
	}
	defer ds.Close()

	workerClient := worker.NewClient(cfg.Renterd.WorkerAddress, cfg.Renterd.WorkerPassword)
	busClient := bus.NewClient(cfg.Renterd.BusAddress, cfg.Renterd.BusPassword)

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "fsd.sqlite3"), log.Named("sqlite"))
	if err != nil {
		log.Fatal("failed to open sqlite database", zap.Error(err))
	}
	defer db.Close()

	bd, err := downloader.NewBlockDownloader(db, cfg.Renterd.Bucket, cfg.BlockStore.CacheSize, cfg.BlockStore.MaxConcurrent, workerClient, log.Named("downloader"))
	if err != nil {
		log.Fatal("failed to create block downloader", zap.Error(err))
	}

	bs, err := renterd.NewBlockStore(
		renterd.WithBucket(cfg.Renterd.Bucket),
		renterd.WithMetadataStore(db),
		renterd.WithWorker(workerClient),
		renterd.WithBus(busClient),
		renterd.WithDownloader(bd),
		renterd.WithLog(log.Named("blockstore")))
	if err != nil {
		log.Fatal("failed to create blockstore", zap.Error(err))
	}

	ipfs, err := ipfs.NewNode(ctx, privateKey, cfg.IPFS, db, ds, bs, log.Named("ipfs"))
	if err != nil {
		log.Fatal("failed to start ipfs node", zap.Error(err))
	}
	defer ipfs.Close()

	apiListener, err := net.Listen("tcp", cfg.API.Address)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer apiListener.Close()

	gatewayListener, err := net.Listen("tcp", cfg.IPFS.Gateway.ListenAddress)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer gatewayListener.Close()

	apiServer := &http.Server{
		Handler: jape.BasicAuth(cfg.API.Password)(shttp.NewAPIHandler(ipfs, cfg, log.Named("api"))),
	}
	defer apiServer.Close()

	gatewayServer := &http.Server{
		Handler: shttp.NewIPFSGatewayHandler(ipfs, cfg, log.Named("gateway")),
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

	buf, err := privateKey.Raw()
	if err != nil {
		log.Fatal("failed to marshal private key", zap.Error(err))
	}
	prettyKey := "ed25519:" + hex.EncodeToString(buf)

	log.Info("fsd started",
		zap.Stringer("peerID", ipfs.PeerID()),
		zap.String("privateKey", prettyKey),
		zap.String("apiAddress", apiListener.Addr().String()),
		zap.String("gatewayAddress", gatewayListener.Addr().String()),
		zap.String("version", build.Version()),
		zap.String("revision", build.Commit()),
		zap.Time("buildTime", build.Time()))

	<-ctx.Done()
}
