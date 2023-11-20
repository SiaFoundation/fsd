package badger

import (
	"context"
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/sia"
	"go.uber.org/zap"
)

// HasBlock returns true if the CID is in the store
func (s *Store) HasBlock(_ context.Context, c cid.Cid) (ok bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(c.Bytes()))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		ok = true
		return nil
	})
	return
}

// GetBlock returns the block metadata for a given CID
func (s *Store) GetBlock(_ context.Context, c cid.Cid) (cm sia.Block, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(c.Bytes()))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return sia.ErrNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &cm)
		})
	})
	return
}

// AddBlocks adds blocks to the store
func (s *Store) AddBlocks(_ context.Context, blocks []sia.Block) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, block := range blocks {
			buf, err := json.Marshal(block)
			if err != nil {
				return err
			} else if err := txn.Set([]byte(block.CID.Bytes()), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

// AllKeysChan returns a channel of all CIDs in the store
func (s *Store) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	go func() {
		log := s.log.Named("allKeysChan")
		_ = s.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{})
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				key := string(it.Item().Key())
				cid, err := cid.Parse(key)
				if err != nil {
					log.Error("failed to parse cid", zap.String("key", key))
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- cid:
				}
			}
			return nil
		})
	}()
	return ch, nil
}
