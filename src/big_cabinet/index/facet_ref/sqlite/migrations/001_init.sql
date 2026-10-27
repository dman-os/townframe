CREATE TABLE IF NOT EXISTS big_cabinet_facet_ref_edges (
    origin_doc TEXT NOT NULL
    , origin_facet TEXT NOT NULL
    , origin_path TEXT NOT NULL
    , target_doc TEXT NOT NULL
    , target_facet TEXT NOT NULL
    , ref_heads BLOB NOT NULL
    , PRIMARY KEY (origin_doc, origin_facet, origin_path, target_doc, target_facet)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_facet_ref_edges_origin_idx
    ON big_cabinet_facet_ref_edges (origin_doc, origin_facet);

CREATE INDEX IF NOT EXISTS big_cabinet_facet_ref_edges_target_idx
    ON big_cabinet_facet_ref_edges (target_doc, target_facet);

CREATE TABLE IF NOT EXISTS big_cabinet_facet_ref_progress (
    facet_tag TEXT PRIMARY KEY NOT NULL
    , revision INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_cabinet_facet_ref_parked (
    origin_doc TEXT NOT NULL
    , origin_facet TEXT NOT NULL
    , revision INTEGER NOT NULL
    , membership BLOB NOT NULL
    , PRIMARY KEY (origin_doc, origin_facet)
) STRICT;
