-- The reverse index of key references, written by the same projection that writes the edges.
-- The `datum_ref` unit's migration also creates this table; both definitions are identical.
-- References by key (`#keyRef`, ADR 035 §2.9). A key reference names no document, so it has no
-- row in `big_cabinet_datum_ref_edges`, whose target is a document and a datum. It is keyed by
-- where it was found: one origin position holds one reference.
CREATE TABLE IF NOT EXISTS big_cabinet_datum_ref_key_edges (
    origin_doc TEXT NOT NULL
    , origin_datum TEXT NOT NULL
    , origin_path TEXT NOT NULL
    , target_key TEXT NOT NULL
    , PRIMARY KEY (origin_doc, origin_datum, origin_path)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_datum_ref_key_edges_target_idx
    ON big_cabinet_datum_ref_key_edges (target_key);
