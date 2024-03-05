package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.sia.tech/fsd/renterd"
	"go.uber.org/zap"
)

func normalizeCid(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
}

func (s *Store) BlockLocation(c cid.Cid) (bucket, key string, err error) {
	c = normalizeCid(c)
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT renterd_bucket, renterd_object_key FROM pinned_blocks pb
INNER JOIN blocks b ON (b.id=pb.block_id) WHERE b.cid=$1`, dbEncode(c)).Scan(&bucket, &key)
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = format.ErrNotFound{Cid: c}
	}
	return
}

func (s *Store) Unpin(c cid.Cid) error {
	c = normalizeCid(c)
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`DELETE FROM pinned_blocks WHERE cid = $1`, dbEncode(c))
		return err
	})
}

func (s *Store) Pin(b renterd.PinnedBlock) error {
	b.Cid = normalizeCid(b.Cid)
	s.log.Debug("pinning block", zap.Stringer("cid", b.Cid), zap.String("bucket", b.Bucket), zap.String("objectKey", b.ObjectKey))
	return s.transaction(func(tx *txn) error {
		insertBlockStmt, err := tx.Prepare(`INSERT INTO blocks (cid, created_at, updated_at) VALUES ($1, $2, $2) ON CONFLICT (cid) DO UPDATE SET updated_at=EXCLUDED.updated_at RETURNING id`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert block statement: %w", err)
		}
		defer insertBlockStmt.Close()

		var parentBlockID int64
		if err := insertBlockStmt.QueryRow(dbEncode(b.Cid), time.Now()).Scan(&parentBlockID); err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}

		_, err = tx.Exec(`INSERT INTO pinned_blocks (block_id, renterd_bucket, renterd_object_key) VALUES ($1, $2, $3)`, parentBlockID, b.Bucket, b.ObjectKey)
		if err != nil {
			return fmt.Errorf("failed to insert pinned block: %w", err)
		}

		if len(b.Links) == 0 {
			return nil
		}

		linkBlockStmt, err := tx.Prepare(`INSERT INTO linked_blocks (parent_id, child_id, link_index) VALUES ($1, $2, $3)`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert linked block statement: %w", err)
		}
		defer linkBlockStmt.Close()

		for i, link := range b.Links {
			link = normalizeCid(link)
			var childBlockID int64
			if err := insertBlockStmt.QueryRow(dbEncode(link), time.Now()).Scan(&childBlockID); err != nil {
				return fmt.Errorf("failed to insert child block %q: %w", link, err)
			} else if _, err := linkBlockStmt.Exec(parentBlockID, childBlockID, i); err != nil {
				return fmt.Errorf("failed to link block %q: %w", link, err)
			}
		}
		return nil
	})
}

// BlockChildren returns the next n children of a given block. If the block
// does not have any children, the returned value should be (nil, nil)
func (s *Store) BlockChildren(c cid.Cid, max int) (children []cid.Cid, err error) {
	c = normalizeCid(c)
	const query = `WITH parent_block AS (SELECT id FROM blocks WHERE cid=$1)
SELECT b.cid
FROM linked_blocks AS lb
INNER JOIN blocks AS b ON (lb.child_id = b.id)
WHERE lb.parent_id = (SELECT id FROM parent_block)
ORDER BY lb.link_index ASC
LIMIT $2`

	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(query, dbEncode(c), max)
		if err != nil {
			return fmt.Errorf("failed to query children: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var child cid.Cid
			if err := rows.Scan(dbDecode(&child)); err != nil {
				return fmt.Errorf("failed to scan child: %w", err)
			}
			children = append(children, child)
		}
		return rows.Err()
	})
	return
}

// BlockSiblings returns the next n siblings of a given block. If the block
// does not have any siblings, the returned value should be (nil, nil)
func (s *Store) BlockSiblings(c cid.Cid, max int) (siblings []cid.Cid, err error) {
	c = normalizeCid(c)
	const query = `WITH child_blocks AS (
	SELECT lb.parent_id, lb.link_index
	FROM linked_blocks AS lb
	INNER JOIN blocks AS b ON (lb.child_id = b.id)
	WHERE b.cid=$1
),
future_siblings AS (
	SELECT lb.child_id
	FROM linked_blocks AS lb
	INNER JOIN child_blocks AS cb ON (lb.parent_id = cb.parent_id)
	WHERE lb.link_index > cb.link_index
	ORDER BY lb.link_index ASC
	LIMIT $2
)
SELECT b.cid
FROM future_siblings AS fs
INNER JOIN blocks AS b ON (b.id = fs.child_id)`

	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(query, dbEncode(c), max)
		if err != nil {
			return fmt.Errorf("failed to query siblings: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var sibling cid.Cid
			if err := rows.Scan(dbDecode(&sibling)); err != nil {
				return fmt.Errorf("failed to scan sibling: %w", err)
			}
			siblings = append(siblings, sibling)
		}
		return rows.Err()
	})
	return
}

// Pinned returns the next n pinned CIDs. If there are no remaining CIDs, the
// returned value should be (nil, nil)
func (s *Store) Pinned(offset, limit int) (roots []cid.Cid, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT b.cid FROM pinned_blocks pb
INNER JOIN blocks b ON (b.id=pb.block_id)
ORDER BY b.id ASC
LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query root cids: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var root cid.Cid
			if err := rows.Scan(dbDecode(&root)); err != nil {
				return fmt.Errorf("failed to scan root cid: %w", err)
			}
			roots = append(roots, root)
		}
		return rows.Err()
	})
	return
}
