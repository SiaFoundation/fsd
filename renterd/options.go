package renterd

import (
	"go.sia.tech/renterd/v2/bus"
	"go.sia.tech/renterd/v2/worker"
	"go.uber.org/zap"
)

type options struct {
	Bucket string

	Downloader BlockDownloader
	Store      MetadataStore
	Worker     *worker.Client
	Bus        *bus.Client
	Log        *zap.Logger
}

// An Option configures a renterd store
type Option func(*options)

// WithMetadataStore sets the metadata store.
func WithMetadataStore(s MetadataStore) Option {
	return func(o *options) {
		o.Store = s
	}
}

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

// WithDownloader sets the block downloader.
func WithDownloader(bd BlockDownloader) Option {
	return func(o *options) {
		o.Downloader = bd
	}
}
