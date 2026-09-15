-- Binds: ?1 rep, ?2 path, ?3 kind, ?4 origin, ?5 content, ?6 target, ?7 avail, ?8 stat, ?9 claim
--
-- Every column but `rep` and `path` is written together, always: a row whose shape disagrees
-- with its kind is not something an update can produce, and a reader that finds one refuses it
-- rather than guessing (`codec::StoredEntry::into_entry`).
INSERT INTO pauperfuse_entry (
      rep
    , path
    , kind
    , origin
    , content
    , target
    , avail
    , stat
    , claim
)
VALUES (
      ?1
    , ?2
    , ?3
    , ?4
    , ?5
    , ?6
    , ?7
    , ?8
    , ?9
)
ON CONFLICT (rep, path) DO UPDATE SET
      kind = excluded.kind
    , origin = excluded.origin
    , content = excluded.content
    , target = excluded.target
    , avail = excluded.avail
    , stat = excluded.stat
    , claim = excluded.claim;
