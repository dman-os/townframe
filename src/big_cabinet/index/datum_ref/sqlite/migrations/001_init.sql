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

CREATE TABLE IF NOT EXISTS big_cabinet_datum_ref_progress (
    datum_tag TEXT PRIMARY KEY NOT NULL
    , revision INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_cabinet_datum_ref_parked (
    origin_doc TEXT NOT NULL
    , origin_datum TEXT NOT NULL
    , revision INTEGER NOT NULL
    , membership BLOB NOT NULL
    , PRIMARY KEY (origin_doc, origin_datum)
) STRICT;
