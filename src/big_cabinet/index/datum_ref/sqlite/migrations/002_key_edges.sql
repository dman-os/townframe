-- The reverse index of key references. The `datum_set` unit's migration also creates this
-- table (idempotently), as it does the edge table, so a store that only ever answers edge
-- queries finds the schema; both definitions are identical.
CREATE TABLE IF NOT EXISTS big_cabinet_datum_ref_key_edges (
    origin_doc TEXT NOT NULL
    , origin_datum TEXT NOT NULL
    , origin_path TEXT NOT NULL
    , target_key TEXT NOT NULL
    , PRIMARY KEY (origin_doc, origin_datum, origin_path)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_datum_ref_key_edges_target_idx
    ON big_cabinet_datum_ref_key_edges (target_key);
