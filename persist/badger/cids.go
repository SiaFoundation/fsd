package badger

import (
	"context"
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
	"github.com/ipfs/go-cid"
	"go.sia.tech/siapfs/ipfs"
)

func (s *Store) HasBlock(cid cid.Cid) (ok bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(cid.Bytes()))
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

func (s *Store) GetBlock(cid cid.Cid) (cm ipfs.Block, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(cid.Bytes()))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ipfs.ErrNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &cm)
		})
	})
	return
}

func (s *Store) AddBlocks(blocks []ipfs.Block) error {
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

func (s *Store) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	go func() {
		_ = s.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{})
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- cid.MustParse(string(it.Item().Key())):
				}
			}
			return nil
		})
	}()
	return ch, nil
}
