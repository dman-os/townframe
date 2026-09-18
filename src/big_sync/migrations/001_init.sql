-- The big_sync schema, for one scope's SQLite database. There is one file for this schema and
-- it is edited in place: it creates fresh databases only, and a database that already applied
-- an earlier revision of it is deliberately unsupported. sqlx records a checksum per applied
-- migration, so the mismatch fails the open (`SqliteCore::init_schema`, and big_repo's own
-- migrator, which runs this same file as its version 1 through the symlink at
-- `big_repo/migrations/001_init.sql`), and `IF NOT EXISTS` below would not add a column to such
-- a database in any case. There is no upgrade path and no backcompat shim.

CREATE TABLE IF NOT EXISTS big_sync_meta (
      key TEXT PRIMARY KEY NOT NULL
    , value INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_sync_scopes (
      scope_id INTEGER PRIMARY KEY AUTOINCREMENT
    , scope_key TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS big_sync_parts (
      part_ref INTEGER PRIMARY KEY
    , scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , part_id BLOB NOT NULL
    , latest_cursor INTEGER NOT NULL DEFAULT 0
    , UNIQUE(scope_id, part_id)
) STRICT;

CREATE TABLE IF NOT EXISTS big_sync_objs (
      obj_ref INTEGER PRIMARY KEY
    , scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , obj_id BLOB NOT NULL
    -- ADR 012 decision 1: the bucket index is a hash of the object key, computed on the
    -- write path. It is stored rather than read off the key's prefix because a bucket's
    -- members are a range of *this* index; it is one column per object, with no fan-out
    -- across the bucket levels the object is a member of.
    , buck_index INTEGER NOT NULL
    , payload_json TEXT
    , UNIQUE(scope_id, obj_id)
    , CHECK(payload_json IS NULL OR json_valid(payload_json))
    , CHECK(buck_index BETWEEN 0 AND 65535)
) STRICT;

-- Membership filters on `buck_index` and still orders by `obj_id`, so the filter leads and
-- the ordering column trails.
CREATE INDEX IF NOT EXISTS big_sync_objs_buck_index_idx
    ON big_sync_objs(scope_id, buck_index, obj_id);

CREATE TABLE IF NOT EXISTS big_sync_buckets (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , part_ref INTEGER NOT NULL REFERENCES big_sync_parts(part_ref)
    , buck_id INTEGER NOT NULL
    , level INTEGER NOT NULL
    , changed_at INTEGER NOT NULL DEFAULT 0
    , live_count INTEGER NOT NULL DEFAULT 0
    , dead_count INTEGER NOT NULL DEFAULT 0
    , live_fp INTEGER NOT NULL DEFAULT 0
    , dead_fp INTEGER NOT NULL DEFAULT 0
    , PRIMARY KEY(scope_id, part_ref, buck_id)
) STRICT;

CREATE INDEX IF NOT EXISTS big_sync_buckets_level_changed_idx
    ON big_sync_buckets(scope_id, part_ref, level, changed_at, buck_id);

-- event_type: 1 = a membership touch (present), 2 = absent. There is no
-- "added" kind: whether an object is new is a fact only the reader's own
-- replica knows, so the substrate reports only touched-or-deleted.
-- `added_at` is the add cursor anyway, and it is what lets a page exclude a
-- tombstone for a reader that never saw the add (`added_at <= cursor < txid`).
-- It is never carried on the wire. The column carries `DEFAULT 0`, but 0 cannot
-- occur: this file only ever creates fresh databases (see the header), so no row
-- predates the column, every add is stamped from the global cursor (bumped before
-- use, so >= 1) and every writer that omits the column only ever reaches its
-- upsert's conflict arm. `added_at <= cursor` would read 0 as "always selected",
-- i.e. deliver the tombstone to every reader; that case is not reachable, so the
-- predicate is a real predicate on every row.
CREATE TABLE IF NOT EXISTS big_sync_members (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , obj_ref INTEGER NOT NULL REFERENCES big_sync_objs(obj_ref)
    , maybe_part_ref INTEGER NOT NULL
    , event_type INTEGER NOT NULL
    , txid INTEGER NOT NULL
    , added_at INTEGER NOT NULL DEFAULT 0
    , PRIMARY KEY(obj_ref, maybe_part_ref)
    , CHECK(maybe_part_ref >= 0)
    , CHECK(event_type BETWEEN 1 AND 2)
    , CHECK(txid >= 0)
    , CHECK(added_at >= 0)
) STRICT;

CREATE INDEX IF NOT EXISTS big_sync_members_part_txid_idx
    ON big_sync_members(scope_id, maybe_part_ref, txid);

CREATE INDEX IF NOT EXISTS big_sync_members_obj_txid_idx
    ON big_sync_members(scope_id, obj_ref, txid);

CREATE TABLE IF NOT EXISTS big_sync_pending_members (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , obj_ref INTEGER NOT NULL REFERENCES big_sync_objs(obj_ref)
    , part_ref INTEGER NOT NULL REFERENCES big_sync_parts(part_ref)
    , PRIMARY KEY(obj_ref, part_ref)
) STRICT;

CREATE TABLE IF NOT EXISTS big_sync_peer_cursors (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , peer_id BLOB NOT NULL
    , part_ref INTEGER NOT NULL REFERENCES big_sync_parts(part_ref)
    , cursor INTEGER NOT NULL
    , PRIMARY KEY(scope_id, peer_id, part_ref)
) STRICT;

CREATE TABLE IF NOT EXISTS big_sync_syncable (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , part_ref INTEGER NOT NULL REFERENCES big_sync_parts(part_ref)
    , principal_id BLOB NOT NULL
    , access_level INTEGER NOT NULL
    , changed_at INTEGER NOT NULL
    , PRIMARY KEY(scope_id, part_ref, principal_id)
) STRICT;

CREATE INDEX IF NOT EXISTS big_sync_syncable_principal_changed_idx
    ON big_sync_syncable(principal_id, changed_at);
