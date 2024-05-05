package sqlite

import (
	"go.uber.org/zap"
)

func migrateV2(tx *txn, _ *zap.Logger) error {
	const query = `ALTER TABLE pinned_blocks ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pinned_blocks ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pinned_blocks ADD COLUMN last_announcement INTEGER NOT NULL DEFAULT 0;
CREATE INDEX pinned_blocks_block_id_last_announcement ON (block_id, last_announcement ASC);`

	_, err := tx.Exec(query)
	return err
}

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx *txn, log *zap.Logger) error{
	migrateV2,
}
