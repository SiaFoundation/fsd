package ipfs

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
)

type (
	// PinnedCID is a CID that needs to be periodically announced.
	PinnedCID struct {
		CID              cid.Cid   `json:"cid"`
		LastAnnouncement time.Time `json:"lastAnnouncement"`
	}

	// A Provider provides CIDs to the IPFS network.
	Provider interface {
		provider.Ready
		provider.ProvideMany
	}

	// A ReprovideStore stores CIDs that need to be periodically announced.
	ReprovideStore interface {
		ProvideCIDs(limit int) ([]PinnedCID, error)
		SetLastAnnouncement(cids []cid.Cid, t time.Time) error
	}
)

// A Reprovider periodically announces CIDs to the IPFS network.
type Reprovider struct {
	provider Provider
	store    ReprovideStore
	log      *zap.Logger

	triggerProvide chan struct{}
}

// Trigger triggers the reprovider loop to run immediately.
func (r *Reprovider) Trigger() {
	select {
	case r.triggerProvide <- struct{}{}:
	default:
	}
}

// Run starts the reprovider loop, which periodically announces CIDs that
// have not been announced in the last interval.
func (r *Reprovider) Run(ctx context.Context, interval time.Duration) {
	var once sync.Once
	once.Do(func() {
		var reprovideSleep time.Duration
		for {
			r.log.Debug("sleeping until next reprovide time", zap.Duration("duration", reprovideSleep))
			select {
			case <-ctx.Done():
				return
			case <-r.triggerProvide:
				r.log.Debug("reprovide triggered")
			case <-time.After(reprovideSleep):
				r.log.Debug("reprovide sleep expired")
			}

			if !r.provider.Ready() {
				r.log.Debug("provider not ready")
				reprovideSleep = time.Minute
				continue
			}

			for {
				cids, err := r.store.ProvideCIDs(1000)
				if err != nil {
					r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
					break
				}

				if len(cids) == 0 {
					r.log.Debug("no CIDs to provide")
					reprovideSleep = 15 * time.Minute // set a minimum sleep time
					break
				}

				nextAnnounce := time.Until(cids[0].LastAnnouncement.Add(interval))

				if nextAnnounce > 0 {
					r.log.Debug("sleeping until next reprovide time", zap.Duration("duration", nextAnnounce))
					reprovideSleep = nextAnnounce
					break
				}

				announced := make([]cid.Cid, 0, len(cids))
				keys := make([]multihash.Multihash, 0, len(cids))
				for _, c := range cids {
					if time.Since(c.LastAnnouncement) < interval {
						reprovideSleep = time.Until(c.LastAnnouncement.Add(interval))
						break
					}

					keys = append(keys, c.CID.Hash())
					announced = append(announced, c.CID)
				}

				ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				defer cancel()

				if err := r.provider.ProvideMany(ctx, keys); err != nil {
					r.log.Error("failed to provide CIDs", zap.Error(err))
					break
				} else if err := r.store.SetLastAnnouncement(announced, time.Now()); err != nil {
					r.log.Error("failed to update last announcement", zap.Error(err))
					break
				}

				r.log.Debug("announced CIDs", zap.Int("count", len(announced)), zap.Stringers("cids", announced))
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
}

// NewReprovider creates a new reprovider.
func NewReprovider(provider Provider, store ReprovideStore, log *zap.Logger) *Reprovider {
	return &Reprovider{
		provider:       provider,
		store:          store,
		log:            log,
		triggerProvide: make(chan struct{}, 1),
	}
}
