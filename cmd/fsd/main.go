package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/libp2p/go-libp2p/core/crypto"
	"go.sia.tech/fsd/build"
	"go.sia.tech/fsd/config"
	shttp "go.sia.tech/fsd/http"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/persist/badger"
	"go.sia.tech/fsd/sia"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"lukechampine.com/frand"
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

	db, err := badger.OpenDatabase(filepath.Join(dir, "fsd.badgerdb"), log.Named("badger"))
	if err != nil {
		log.Fatal("failed to open badger database", zap.Error(err))
	}
	defer db.Close()

	var privateKey crypto.PrivKey
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
	store := sia.NewBlockStore(db, cfg.Renterd, log.Named("blockstore"))

	inode, err := ipfs.NewNode(ctx, privateKey, cfg.IPFS, store)
	if err != nil {
		log.Fatal("failed to start ipfs node", zap.Error(err))
	}
	defer inode.Close()

	snode := sia.New(db, cfg.Renterd, log.Named("sia"))

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
		Handler: jape.BasicAuth(cfg.API.Password)(shttp.NewAPIHandler(inode, snode, cfg, log.Named("api"))),
	}
	defer apiServer.Close()

	gatewayServer := &http.Server{
		Handler: shttp.NewIPFSGatewayHandler(inode, snode, cfg, log.Named("gateway")),
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
		zap.Stringer("peerID", inode.PeerID()),
		zap.String("privateKey", prettyKey),
		zap.String("apiAddress", apiListener.Addr().String()),
		zap.String("gatewayAddress", gatewayListener.Addr().String()),
		zap.String("version", build.Version()),
		zap.String("revision", build.Commit()),
		zap.Time("buildTime", build.Time()))

	<-ctx.Done()
}
