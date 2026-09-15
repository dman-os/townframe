-- Reps: one row per backend whose state the bridge has recorded.
--
-- `generation` moves once per rep update, which is what makes a rep update observable to a
-- reader that holds a page from before it. `updated_at` is milliseconds since the epoch, for
-- humans; nothing decides anything from it.
CREATE TABLE IF NOT EXISTS pauperfuse_rep (
      name TEXT PRIMARY KEY
    , generation INTEGER NOT NULL
    , updated_at INTEGER NOT NULL
) STRICT;

-- One recorded path per rep, and nothing else: no bytes, no history (ADR 010 §2.2, §5.1).
--
-- `path` holds `pauperfuse::codec::encode_path` output and must never hold any other encoding.
-- `RelPath`'s ordering and this column's memcmp ordering are the same relation, which is what
-- makes the primary key the walk order: a page of a scan is a range scan with no sort, and a
-- comparison between two reps is a merge join over two of those scans.
--
-- `WITHOUT ROWID` makes that key the table itself, so an ordered scan is one index walk with no
-- rowid indirection.
--
-- The payload columns are nullable per kind, and the store refuses a row whose shape does not
-- match its kind (`codec::StoredEntry::into_entry`):
--
--   `origin`  a file's identity: an opaque token, or the recipe that regenerates the bytes
--   `content` the digest of bytes this device has had in hand: evidence, never required
--   `target`  a symlink's target, verbatim
--   `stat`    the fingerprint a scan compares against, absent for directories, whose size and
--             mtime are derived from children each recorded on their own
--
-- A directory is therefore `kind = 1` with no origin, no content, no target and no stat: its
-- identity is that it exists.
--
-- `ON DELETE CASCADE` is what guarantees a dropped rep takes its entries with it. That requires
-- `PRAGMA foreign_keys = ON`, which `SqlCtx` sets on every connection it opens;
-- `dropping_a_rep_drops_its_entries` pins it.
CREATE TABLE IF NOT EXISTS pauperfuse_entry (
      rep TEXT NOT NULL REFERENCES pauperfuse_rep (name) ON DELETE CASCADE
    , path BLOB NOT NULL
    , kind INTEGER NOT NULL
    , origin BLOB
    , content BLOB
    , target BLOB
    , avail INTEGER NOT NULL
    , stat BLOB
    , claim BLOB
    , PRIMARY KEY (rep, path)
) STRICT, WITHOUT ROWID;
