// package downloader contains a cache for downloading blocks from a renterd node.
// A cache optimizes the number of in-flight requests to avoid overloading the
// node and caches blocks to avoid redundant downloads.
package downloader

import (
	"bytes"
	"container/heap"
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

const (
	downloadPriorityLow = iota + 1
	downloadPriorityMedium
	downloadPriorityHigh
)

type (
	blockResponse struct {
		ch  chan struct{}
		b   []byte
		err error

		key      string
		priority int
		index    int
	}

	priorityQueue []*blockResponse

	// BlockDownloader is a cache for downloading blocks from a renterd node.
	// It limits the number of in-flight requests to avoid overloading the node
	// and caches blocks to avoid redundant downloads.
	//
	// For UnixFS nodes, it also prefetches linked blocks.
	BlockDownloader struct {
		workerClient *worker.Client
		log          *zap.Logger

		bucket string

		ch chan struct{}

		mu    sync.Mutex // protects the fields below
		cache *lru.TwoQueueCache[string, *blockResponse]
		queue *priorityQueue
	}
)

func (h priorityQueue) Len() int { return len(h) }

func (h priorityQueue) Less(i, j int) bool {
	return h[i].priority < h[j].priority
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
	return blocks.NewBlockWithCid(br.b, c)
}

func (bd *BlockDownloader) doDownloadTask(task *blockResponse, log *zap.Logger) {
	defer func() {
		close(task.ch)
	}()
	log.Debug("downloading block")
	start := time.Now()
	blockBuf := bytes.NewBuffer(make([]byte, 0, 1<<20))

	err := bd.workerClient.DownloadObject(context.Background(), blockBuf, bd.bucket, task.key, api.DownloadObjectOptions{
		Range: api.DownloadRange{
			Offset: 0,
			Length: 1 << 20,
		},
	})
	if err != nil {
		log.Debug("failed to download block", zap.Error(err))
		task.err = err
		bd.cache.Remove(task.key)
		return
	}

	task.b = blockBuf.Bytes()

	log.Debug("downloaded block", zap.Duration("elapsed", time.Since(start)))
	pn, err := merkledag.DecodeProtobuf(task.b)
	if err != nil {
		log.Debug("block is not a ProtoNode", zap.Error(err))
	}

	if len(pn.Links()) == 0 {
		return
	}

	// prefetch linked blocks
	links := pn.Links()
	if len(links) > 2 {
		// prioritize first and last linked blocks
		firstLink := links[0]
		lastLink := links[len(links)-1]
		links = links[1 : len(links)-1]

		bd.getResponse(firstLink.Cid, downloadPriorityMedium)
		bd.getResponse(lastLink.Cid, downloadPriorityMedium)
	}
	for _, link := range links {
		bd.getResponse(link.Cid, downloadPriorityLow)
		log.Debug("queued linked blocks", zap.Stringer("cid", link.Cid), zap.String("key", cidKey(link.Cid)))
	}
}

func (bd *BlockDownloader) getResponse(c cid.Cid, priority int) *blockResponse {
	bd.mu.Lock()
	defer bd.mu.Unlock()
	key := cidKey(c)

	if task, ok := bd.cache.Get(key); ok {
		bd.log.Debug("cache hit", zap.String("key", key))
		return task
	}
	task := &blockResponse{
		key:      key,
		priority: priority,
		ch:       make(chan struct{}),
	}
	bd.cache.Add(key, task)
	heap.Push(bd.queue, task)
	bd.ch <- struct{}{}
	return task
}

func (bd *BlockDownloader) downloadWorker(ctx context.Context, n int) {
	log := bd.log.Named("worker").With(zap.Int("n", n))

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
		bd.mu.Unlock()
		bd.doDownloadTask(task, log.With(zap.String("key", task.key)))
	}
}

// Get returns a block by CID.
func (bd *BlockDownloader) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return bd.getResponse(c, downloadPriorityHigh).block(ctx, c)
}

func cidKey(c cid.Cid) string {
	return cid.NewCidV1(c.Type(), c.Hash()).String()
}

// NewBlockDownloader creates a new BlockDownloader.
func NewBlockDownloader(bucket string, cacheSize, workers int, workerClient *worker.Client, log *zap.Logger) (*BlockDownloader, error) {
	cache, err := lru.New2Q[string, *blockResponse](cacheSize)
	if err != nil {
		return nil, err
	}

	bd := &BlockDownloader{
		workerClient: workerClient,
		log:          log,
		cache:        cache,
		queue:        &priorityQueue{},

		ch:     make(chan struct{}, 1000),
		bucket: bucket,
	}
	heap.Init(bd.queue)
	for i := 0; i < workers; i++ {
		go bd.downloadWorker(context.Background(), i)
	}
	return bd, nil
}
