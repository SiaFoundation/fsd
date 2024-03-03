package renterd

import (
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

type options struct {
	Bucket    string
	CacheSize int

	Downloader BlockDownloader
	Worker     *worker.Client
	Bus        *bus.Client
	Log        *zap.Logger
}

type Option func(*options)

// WithBucket sets the bucket name.
func WithBucket(bucket string) Option {
	return func(o *options) {
		o.Bucket = bucket
	}
}

// WithLog sets the logger.
func WithLog(l *zap.Logger) Option {
	return func(o *options) {
		o.Log = l
	}
}

// WithWorker sets the worker client.
func WithWorker(w *worker.Client) Option {
	return func(o *options) {
		o.Worker = w
	}
}

// WithBus sets the bus client.
func WithBus(b *bus.Client) Option {
	return func(o *options) {
		o.Bus = b
	}
}

func WithDownloader(bd BlockDownloader) Option {
	return func(o *options) {
		o.Downloader = bd
	}
}

// WithCacheSize sets the size of the block cache.
func WithCacheSize(size int) Option {
	return func(o *options) {
		o.CacheSize = size
	}
}
