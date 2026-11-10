-- Reference-tag projection bookkeeping, and the shared reference-edge table.
--
-- The datum_set worker is now the only writer of `big_cabinet_datum_ref_edges`:
-- it reads each document once and, from that single read, derives both the
-- predicate entries and the outgoing declared references. This table records
-- which reference tags have already been backfilled from *stored* entries, so a
-- tag activated later does not re-extract documents whose stored entry predates
-- the tag. Backfill reads each affected stored document once and extracts each
-- entry at the `doc_heads` stored beside it -- the revision that entry itself
-- describes -- so the edges agree with the membership next to them. A tag is
-- recorded only once every document holding its entries was read, which is what
-- makes per-document bookkeeping unnecessary here.
CREATE TABLE IF NOT EXISTS big_cabinet_datum_set_ref_tags (
    tag TEXT PRIMARY KEY NOT NULL
) STRICT;

-- The reverse edge index. The `datum_ref` unit's migration also creates this
-- table (idempotently) so a store that only ever answers edge queries on a
-- fresh database still finds the schema; both definitions are identical.
CREATE TABLE IF NOT EXISTS big_cabinet_datum_ref_edges (
    origin_doc TEXT NOT NULL
    , origin_datum TEXT NOT NULL
    , origin_path TEXT NOT NULL
    , target_doc TEXT NOT NULL
    , target_datum TEXT NOT NULL
    , ref_heads BLOB NOT NULL
    , PRIMARY KEY (origin_doc, origin_datum, origin_path, target_doc, target_datum)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_datum_ref_edges_origin_idx
    ON big_cabinet_datum_ref_edges (origin_doc, origin_datum);

CREATE INDEX IF NOT EXISTS big_cabinet_datum_ref_edges_target_idx
    ON big_cabinet_datum_ref_edges (target_doc, target_datum);
