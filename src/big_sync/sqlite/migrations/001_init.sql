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
    , buck_index INTEGER NOT NULL
    , payload_json TEXT
    , UNIQUE(scope_id, obj_id)
    , CHECK(payload_json IS NULL OR json_valid(payload_json))
    , CHECK(buck_index BETWEEN 0 AND 65535)
) STRICT;

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
