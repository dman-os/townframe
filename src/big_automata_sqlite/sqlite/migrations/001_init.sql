-- The part store. Keys (objects, parts, principals) are stored as order-preserving blobs: see
-- `keys.rs`. Every table is keyed so that the reads of `PartStoreServe` are index ranges.

-- One row: the rev of the last committed transaction that stamped anything, and the store's
-- generation (see `big_automata::part_store::Generation`): the identity of this log, the 16
-- bytes of a UUID. It is minted here, by the migration, so a database has a generation from
-- the moment it has a log and no open ever has to decide whether to mint. `mint_generation`
-- replaces it. A copy of the file carries it along, which is why a restore needs the
-- operator's mint.
CREATE TABLE big_automata_meta (
      id INTEGER PRIMARY KEY CHECK (id = 1)
    , rev INTEGER NOT NULL
    , generation BLOB NOT NULL CHECK (length(generation) = 16)
) STRICT;

INSERT INTO big_automata_meta (id, rev, generation) VALUES (1, 0, randomblob(16));

CREATE TABLE big_automata_parts (
      part_key BLOB PRIMARY KEY
      -- the rev of the part's latest member row; access rows do not move it
    , latest_cursor INTEGER NOT NULL DEFAULT 0
) STRICT, WITHOUT ROWID;

CREATE TABLE big_automata_objs (
      obj_key BLOB PRIMARY KEY
    , payload TEXT NOT NULL CHECK (json_valid(payload))
      -- the payload's PayloadDigest, computed once when the payload was written
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
      -- live memberships plus pending ones, maintained by every write that moves a membership,
      -- so the objects that are members of no part are one partial index range
    , part_count INTEGER NOT NULL DEFAULT 0
) STRICT, WITHOUT ROWID;

CREATE INDEX big_automata_objs_partless
    ON big_automata_objs (obj_key)
 WHERE part_count = 0;

-- The log's member rows, collapsed to the latest write: a removed member is a tombstone
-- (live = 0), kept so the removal reaches readers behind it. `buck` is the object's deepest
-- bucket index, so a bucket's members are one key range.
CREATE TABLE big_automata_members (
      part_key BLOB NOT NULL
    , buck INTEGER NOT NULL CHECK (buck BETWEEN 0 AND 65535)
    , obj_key BLOB NOT NULL
    , live INTEGER NOT NULL CHECK (live IN (0, 1))
    , rev INTEGER NOT NULL
    , added_at INTEGER NOT NULL
    , PRIMARY KEY (part_key, buck, obj_key)
) STRICT, WITHOUT ROWID;

-- A part route: the part's rows past a rev, in (rev, key) order; dirt counts.
CREATE INDEX big_automata_members_by_part_rev
    ON big_automata_members (part_key, rev, obj_key);
-- An object route and an object's parts.
CREATE INDEX big_automata_members_by_obj_rev
    ON big_automata_members (obj_key, rev, part_key);
-- The whole log in rev order.
CREATE INDEX big_automata_members_by_rev
    ON big_automata_members (rev, obj_key, part_key);

-- Memberships added before the object had a payload: no log row, no bucket entry.
CREATE TABLE big_automata_pending (
      obj_key BLOB NOT NULL
    , part_key BLOB NOT NULL
    , PRIMARY KEY (obj_key, part_key)
) STRICT, WITHOUT ROWID;

-- The log's access rows; a revocation is a tombstone (level NULL).
CREATE TABLE big_automata_access (
      part_key BLOB NOT NULL
    , principal BLOB NOT NULL
    , level INTEGER CHECK (level IS NULL OR level BETWEEN 0 AND 3)
    , rev INTEGER NOT NULL
    , PRIMARY KEY (part_key, principal)
) STRICT, WITHOUT ROWID;

CREATE INDEX big_automata_access_by_rev
    ON big_automata_access (rev, part_key, principal);

-- Each part's bucket summaries at every level, maintained by the write that moves a member.
-- `buck_id` is `BuckId`'s packed (level << 16 | index), so a range of ids is a range of
-- levels. The fingerprints are u64 wrapping sums stored as their i64 bit pattern.
CREATE TABLE big_automata_buckets (
      part_key BLOB NOT NULL
    , buck_id INTEGER NOT NULL
    , changed_at INTEGER NOT NULL
    , live_count INTEGER NOT NULL
    , dead_count INTEGER NOT NULL
    , live_fp INTEGER NOT NULL
    , dead_fp INTEGER NOT NULL
    , PRIMARY KEY (part_key, buck_id)
) STRICT, WITHOUT ROWID;

-- The driver's `PeerCursors`, which stores a `PeerCursor`: the rev and the generation of the
-- peer's log it is a position in, together and both required. Last-write-wins: a write
-- replaces the row whatever its rev or generation.
CREATE TABLE big_automata_peer_cursors (
      peer BLOB NOT NULL
    , backend TEXT NOT NULL
    , part_key BLOB NOT NULL
    , cursor INTEGER NOT NULL
    , generation BLOB NOT NULL CHECK (length(generation) = 16)
    , PRIMARY KEY (peer, backend, part_key)
) STRICT, WITHOUT ROWID;

-- The okv tables (`big_automata::okv`), in the same database as the part store so one write
-- transaction can stage both. A namespace is one okv table and owns its own rev counter, which
-- an `apply` bumps in the caller's transaction.
CREATE TABLE big_automata_okv_namespaces (
      namespace_id INTEGER PRIMARY KEY
    , name TEXT NOT NULL UNIQUE
    , rev INTEGER NOT NULL
) STRICT;

-- One row per key: the rev of the batch that last wrote it, and its value, NULL for a
-- tombstone. Keys are raw bytes: SQLite's blob order is the memcmp order of `Vec<u8>`.
CREATE TABLE big_automata_okv_entries (
      namespace_id INTEGER NOT NULL
    , key BLOB NOT NULL
    , rev INTEGER NOT NULL
    , value BLOB
    , PRIMARY KEY (namespace_id, key)
) STRICT, WITHOUT ROWID;

-- The paged reads: a namespace's rows in `(rev, key)` order.
CREATE INDEX big_automata_okv_entries_by_rev
    ON big_automata_okv_entries (namespace_id, rev, key);

-- Pruning: a namespace's tombstones by rev.
CREATE INDEX big_automata_okv_entries_tombstones
    ON big_automata_okv_entries (namespace_id, rev)
 WHERE value IS NULL;

-- The consumers holding tombstones back, and the rev each has gone past.
CREATE TABLE big_automata_okv_consumers (
      namespace_id INTEGER NOT NULL
    , consumer TEXT NOT NULL
    , cursor INTEGER NOT NULL
    , PRIMARY KEY (namespace_id, consumer)
) STRICT, WITHOUT ROWID;
