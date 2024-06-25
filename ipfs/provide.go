package ipfs

import (
	"context"
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
func (r *Reprovider) Run(ctx context.Context, interval, timeout time.Duration, batchSize int) {
	var reprovideSleep time.Duration

	for {
		if r.provider.Ready() {
			break
		}
		r.log.Debug("provider not ready")
		time.Sleep(30 * time.Second)
	}

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

		doProvide := func(ctx context.Context, keys []multihash.Multihash) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return r.provider.ProvideMany(ctx, keys)
		}

		for {
			start := time.Now()

			cids, err := r.store.ProvideCIDs(batchSize)
			if err != nil {
				reprovideSleep = time.Minute
				r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
				break
			} else if len(cids) == 0 {
				reprovideSleep = 10 * time.Minute
				r.log.Debug("reprovide complete")
				break
			}

			rem := time.Until(cids[0].LastAnnouncement.Add(interval))
			if rem > 0 {
				reprovideSleep = rem
				r.log.Debug("reprovide complete")
				break
			}

			announced := make([]cid.Cid, 0, len(cids))
			keys := make([]multihash.Multihash, 0, len(cids))
			// include a slight buffer for CIDs that are about to expire
			// so they will be provided as one batch
			buffer := interval / 10
			minAnnouncement := time.Now().Add(-(interval - buffer))
			for _, c := range cids {
				// only provide CIDs that have not been provided within the
				// last interval
				if c.LastAnnouncement.After(minAnnouncement) {
					break
				}
				keys = append(keys, c.CID.Hash())
				announced = append(announced, c.CID)
			}

			if err := doProvide(ctx, keys); err != nil {
				reprovideSleep = time.Minute
				r.log.Error("failed to provide CIDs", zap.Error(err))
				break
			} else if err := r.store.SetLastAnnouncement(announced, time.Now()); err != nil {
				reprovideSleep = time.Minute
				r.log.Error("failed to update last announcement time", zap.Error(err))
				break
			}
			r.log.Debug("provided CIDs", zap.Int("count", len(announced)), zap.Duration("elapsed", time.Since(start)))
		}
	}
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
