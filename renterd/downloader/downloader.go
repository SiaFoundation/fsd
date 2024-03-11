// package downloader contains a cache for downloading blocks from a renterd node.
// A cache optimizes the number of in-flight requests to avoid overloading the
// node and caches blocks to avoid redundant downloads.
package downloader

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

const (
	downloadPriorityLow = iota + 1
	downloadPriorityMedium
	downloadPriorityHigh
	downloadPriorityMax
)

type (
	downloadPriority int8

	blockResponse struct {
		ch  chan struct{}
		b   []byte
		err error

		cid       cid.Cid
		priority  downloadPriority
		index     int
		timestamp time.Time
	}

	priorityQueue []*blockResponse

	// A MetadataStore is a store for IPFS block metadata. It is used to
	// optimize block downloads by prefetching linked blocks.
	MetadataStore interface {
		BlockLocation(c cid.Cid) (bucket, key string, err error)
		BlockSiblings(c cid.Cid, max int) (siblings []cid.Cid, err error)
		BlockChildren(c cid.Cid, max int) (siblings []cid.Cid, err error)
	}

	// BlockDownloader is a cache for downloading blocks from a renterd node.
	// It limits the number of in-flight requests to avoid overloading the node
	// and caches blocks to avoid redundant downloads.
	//
	// For UnixFS nodes, it also prefetches linked blocks.
	BlockDownloader struct {
		store        MetadataStore
		workerClient *worker.Client
		log          *zap.Logger

		bucket string

		ch chan struct{}

		mu    sync.Mutex // protects the fields below
		cache *lru.TwoQueueCache[string, *blockResponse]
		queue *priorityQueue
	}
)

func (dp downloadPriority) String() string {
	switch dp {
	case downloadPriorityLow:
		return "low"
	case downloadPriorityMedium:
		return "medium"
	case downloadPriorityHigh:
		return "high"
	case downloadPriorityMax:
		return "max"
	default:
		panic("invalid download priority")
	}
}

func (h priorityQueue) Len() int { return len(h) }

func (h priorityQueue) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].timestamp.Before(h[j].timestamp)
}

func (h priorityQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *priorityQueue) Push(t any) {
	n := len(*h)
	task := t.(*blockResponse)
	task.index = n
	*h = append(*h, task)
}

func (h *priorityQueue) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*h = old[0 : n-1]
	return item
}

var _ heap.Interface = &priorityQueue{}

func (br *blockResponse) block(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-br.ch:
	}
	if br.err != nil {
		return nil, br.err
	}
	return blocks.NewBlockWithCid(br.b, c)
}

func (bd *BlockDownloader) doDownloadTask(task *blockResponse, log *zap.Logger) {
	log = log.Named("doDownloadTask").With(zap.Stringer("cid", task.cid), zap.Stringer("priority", task.priority))
	blockBuf := bytes.NewBuffer(make([]byte, 0, 2<<20))

	start := time.Now()
	bucket, key, err := bd.store.BlockLocation(task.cid)
	if err != nil {
		log.Error("failed to get block location", zap.Error(err))
		task.err = err
		bd.cache.Remove(cidKey(task.cid))
		close(task.ch)
		return
	}

	log.Debug("downloading block", zap.String("bucket", bucket), zap.String("key", key))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err = bd.workerClient.DownloadObject(ctx, blockBuf, bucket, key, api.DownloadObjectOptions{})
	if err != nil {
		log.Error("failed to download block", zap.Error(err))
		task.err = err
		bd.cache.Remove(cidKey(task.cid))
		close(task.ch)
		return
	}

	c := task.cid
	h, err := mh.Sum(blockBuf.Bytes(), c.Prefix().MhType, -1)
	if err != nil {
		log.Error("failed to hash block", zap.Error(err))
		task.err = err
		bd.cache.Remove(cidKey(task.cid))
		close(task.ch)
		return
	} else if c.Hash().HexString() != h.HexString() {
		log.Error("block hash mismatch", zap.String("expected", c.Hash().HexString()), zap.String("actual", h.HexString()))
		task.err = errors.New("block hash mismatch")
		bd.cache.Remove(cidKey(task.cid))
		close(task.ch)
		return
	}
	log.Info("downloaded block", zap.Duration("elapsed", time.Since(start)))
	task.b = blockBuf.Bytes()
	close(task.ch)
}

func (bd *BlockDownloader) getResponse(c cid.Cid, priority downloadPriority) *blockResponse {
	bd.mu.Lock()
	defer bd.mu.Unlock()
	key := cidKey(c)

	if task, ok := bd.cache.Get(key); ok {
		bd.log.Debug("cache hit", zap.String("key", key))
		// update the priority if the task is still queued
		if task.priority < priority && task.index != -1 {
			task.priority = priority
			heap.Fix(bd.queue, task.index)
		}
		return task
	}
	task := &blockResponse{
		cid:       c,
		priority:  priority,
		timestamp: time.Now(),
		ch:        make(chan struct{}),
	}
	bd.cache.Add(key, task)
	heap.Push(bd.queue, task)
	go func() {
		bd.ch <- struct{}{}
	}()
	return task
}

func (bd *BlockDownloader) downloadWorker(ctx context.Context, n int) {
	log := bd.log.Named("worker").With(zap.Int("id", n))
	for {
		select {
		case <-ctx.Done():
			return
		case <-bd.ch:
		}

		bd.mu.Lock()
		if bd.queue.Len() == 0 {
			bd.mu.Unlock()
			continue
		}

		task := heap.Pop(bd.queue).(*blockResponse)
		log := log.With(zap.Stringer("cid", task.cid), zap.Stringer("priority", task.priority))
		log.Debug("popped task from queue")
		bd.mu.Unlock()
		bd.doDownloadTask(task, log)
	}
}

// Get returns a block by CID.
func (bd *BlockDownloader) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	log := bd.log.Named("Get").With(zap.Stringer("cid", c))
	resp := bd.getResponse(c, downloadPriorityMax)
	go func() {
		// prefetch children
		children, err := bd.store.BlockChildren(c, 10)
		if err != nil {
			log.Error("failed to get block children", zap.Error(err))
		} else if len(children) > 0 {
			log.Debug("prefetching children", zap.Int("count", len(children)))
		}
		for _, child := range children {
			bd.getResponse(child, downloadPriorityLow)
		}

		// prefetch siblings
		siblings, err := bd.store.BlockSiblings(c, 50)
		if err != nil {
			log.Error("failed to get block siblings", zap.Error(err))
		} else if len(siblings) > 0 {
			log.Debug("prefetching siblings", zap.Int("count", len(siblings)))
		}
		for _, sibling := range siblings {
			bd.getResponse(sibling, downloadPriorityMedium)
		}
	}()
	return resp.block(ctx, c)
}

func cidKey(c cid.Cid) string {
	return cid.NewCidV1(c.Type(), c.Hash()).String()
}

// NewBlockDownloader creates a new BlockDownloader.
func NewBlockDownloader(store MetadataStore, bucket string, cacheSize, workers int, workerClient *worker.Client, log *zap.Logger) (*BlockDownloader, error) {
	cache, err := lru.New2Q[string, *blockResponse](cacheSize)
	if err != nil {
		return nil, err
	}

	bd := &BlockDownloader{
		store:        store,
		workerClient: workerClient,
		log:          log,
		cache:        cache,
		queue:        &priorityQueue{},

		ch:     make(chan struct{}, workers),
		bucket: bucket,
	}
	heap.Init(bd.queue)
	for i := 0; i < workers; i++ {
		go bd.downloadWorker(context.Background(), i)
	}
	return bd, nil
}
