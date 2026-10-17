-- name: upsert_edge :exec
-- param: origin_doc_id String
-- param: origin_facet_key String
-- param: target_doc_id String
-- param: target_facet_key String
-- param: reference_kind String
-- param: origin_heads String
INSERT INTO facet_ref_edges (
    origin_doc_id
  , origin_facet_key
  , target_doc_id
  , target_facet_key
  , reference_kind
  , origin_heads
) VALUES (:origin_doc_id, :origin_facet_key, :target_doc_id, :target_facet_key, :reference_kind, :origin_heads)
ON CONFLICT(origin_doc_id, origin_facet_key, target_doc_id, target_facet_key, reference_kind)
DO UPDATE SET origin_heads = excluded.origin_heads
