CREATE TABLE IF NOT EXISTS facet_ref_edges (
    origin_doc_id TEXT NOT NULL
  , origin_facet_key TEXT NOT NULL
  , target_doc_id TEXT NOT NULL
  , target_facet_key TEXT NOT NULL
  , reference_kind TEXT NOT NULL
  , origin_heads TEXT NOT NULL
  , PRIMARY KEY(origin_doc_id, origin_facet_key, target_doc_id, target_facet_key, reference_kind)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_target ON facet_ref_edges(target_doc_id, target_facet_key);
CREATE INDEX IF NOT EXISTS idx_facet_ref_edges_origin ON facet_ref_edges(origin_doc_id, origin_facet_key);
