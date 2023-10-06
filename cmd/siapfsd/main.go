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

	"go.sia.tech/renterd/worker"
	"go.sia.tech/siapfs/api"
	"go.sia.tech/siapfs/persist/badger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	dir               = "."
	bucket            = "ipfs"
	workerCredentials = api.RenterdCredentials{
		Address:  "http://localhost:9980/api/worker",
		Password: "password",
	}
	apiAddr = "localhost:8001"
)

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
	flag.StringVar(&bucket, "renterd.bucket", bucket, "bucket to use for renterd")
	flag.StringVar(&workerCredentials.Address, "renterd.addr", workerCredentials.Address, "worker address to use for renterd")
	flag.StringVar(&workerCredentials.Password, "renterd.pass", workerCredentials.Password, "worker password to use for renterd")
	flag.Parse()

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

	client := worker.NewClient(workerCredentials.Address, workerCredentials.Password)
	coreAPI, node, err := createNode(ctx, ipfsPath, ds, client, bucket)
	if err != nil {
		log.Fatal("failed to start ipfs node", zap.Error(err))
	}
	defer node.Close()

	l, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer l.Close()

	server := &http.Server{
		Handler: api.NewServer(bucket, workerCredentials, coreAPI, ds, log.Named("api")),
	}
	defer server.Close()
	log.Info("listening", zap.String("addr", l.Addr().String()))

	go func() {
		if err := server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("failed to serve", zap.Error(err))
		}
	}()

	<-ctx.Done()
}
