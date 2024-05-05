CREATE TABLE blocks (
	id INTEGER PRIMARY KEY NOT NULL, -- the id of the block
	cid TEXT UNIQUE NOT NULL, -- the cid of the block
	created_at INTEGER NOT NULL, -- the time the block was created
	updated_at INTEGER NOT NULL -- the time the block was last updated
);

CREATE TABLE linked_blocks (
	parent_id INTEGER REFERENCES blocks(id), 
	child_id INTEGER REFERENCES blocks(id), 
	link_index INTEGER NOT NULL, -- the index of the child block in the parent block
	UNIQUE(parent_id, link_index)
);
CREATE INDEX linked_blocks_parent_id_index ON linked_blocks (parent_id);
CREATE INDEX linked_blocks_child_id_index ON linked_blocks (child_id);
CREATE INDEX linked_blocks_parent_id_child_id_link_index ON linked_blocks (parent_id, child_id, link_index);

CREATE TABLE pinned_blocks (
	block_id INTEGER PRIMARY KEY REFERENCES blocks(id),
	renterd_bucket TEXT NOT NULL, -- the bucket of the block in the object store
	renterd_object_key TEXT NOT NULL, -- the key of the block in the object store
	created_at INTEGER NOT NULL, -- the time the block was created
	updated_at INTEGER NOT NULL, -- the time the block was last updated
	last_announcement INTEGER NOT NULL, -- the time the block was last announced
	UNIQUE(renterd_bucket, renterd_object_key)
);
CREATE INDEX pinned_blocks_block_id_last_announcement ON pinned_blocks (block_id, last_announcement ASC);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL -- used for migrations
);
