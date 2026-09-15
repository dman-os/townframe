-- Willow entry storage.
--
-- The schema is deliberately generic: it knows namespaces, subspaces, paths, timestamps and
-- payloads, and nothing about any embedder's concepts. `scope_id` is the only storage-level
-- concession, so several embedder instances can share one database.

CREATE TABLE IF NOT EXISTS big_willow_scopes (
      scope_id INTEGER PRIMARY KEY AUTOINCREMENT
    , scope_key TEXT NOT NULL UNIQUE
) STRICT;

-- One retained entry per (scope, namespace, subspace, path).
--
-- `path` holds `big_willow::encode_path` output and must never hold any other encoding.
-- `big_willow::prefix_range` is exact only over that encoding, and pruning is expressed as a
-- range query over this column, so a different encoding here would silently delete or admit
-- the wrong entries. See the comment on `prefix_range`.
--
-- `timestamp` and `payload_length` hold the big-endian bytes of a `u64`, not INTEGER.
-- Recency is `(timestamp, payload_digest, payload_length)` compared as unsigned integers, and
-- an INTEGER column would compare as signed: a peer-supplied timestamp above `i64::MAX` would
-- wrap negative and sort below every real entry, so it would never prune anything and would be
-- pruned by entries it is newer than. Eight big-endian bytes make SQLite's memcmp ordering
-- match Rust's `u64` ordering across the whole range.
--
-- `timestamp`, `payload_digest` and `payload_length` are denormalised out of `entry` so that
-- recency and time bounds are expressible in SQL. They must always agree with the decoded
-- entry; the store writes all four together and never updates them independently.
--
-- WITHOUT ROWID makes the primary key the table itself, so the ordered scan is one index walk
-- with no rowid indirection.
CREATE TABLE IF NOT EXISTS big_willow_entries (
      scope_id INTEGER NOT NULL REFERENCES big_willow_scopes(scope_id)
    , namespace BLOB NOT NULL
    , subspace BLOB NOT NULL
    , path BLOB NOT NULL
    , timestamp BLOB NOT NULL
    , payload_digest BLOB NOT NULL
    , payload_length BLOB NOT NULL
    , entry BLOB NOT NULL
    , PRIMARY KEY (scope_id, namespace, subspace, path)
) STRICT, WITHOUT ROWID;

-- The payload retained for an entry, or no row at all when the payload has not arrived.
--
-- A separate table keeps payload bytes out of the ordered scan, and expresses "payload not
-- received" as a missing row rather than a nullability convention.
--
-- ON DELETE CASCADE is what guarantees a payload row never outlives its entry, including when
-- pruning removes an entry. That requires `PRAGMA foreign_keys = ON`, which `SqlCtx` sets on
-- every connection it opens; `a_payload_never_outlives_its_entry` pins it.
CREATE TABLE IF NOT EXISTS big_willow_payloads (
      scope_id INTEGER NOT NULL
    , namespace BLOB NOT NULL
    , subspace BLOB NOT NULL
    , path BLOB NOT NULL
    , payload BLOB NOT NULL
    , PRIMARY KEY (scope_id, namespace, subspace, path)
    , FOREIGN KEY (scope_id, namespace, subspace, path)
          REFERENCES big_willow_entries (scope_id, namespace, subspace, path)
          ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
