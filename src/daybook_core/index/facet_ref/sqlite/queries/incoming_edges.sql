-- name: incoming_edges :many
-- param: target_doc_id String
-- param: target_facet_key String
-- into: crate::index::facet_ref::EdgeRow
-- returns: origin_doc_id String, origin_facet_key String, target_doc_id String, target_facet_key String, reference_kind String, origin_heads String
SELECT origin_doc_id
     , origin_facet_key
     , target_doc_id
     , target_facet_key
     , reference_kind
     , origin_heads
  FROM facet_ref_edges
 WHERE target_doc_id = :target_doc_id AND target_facet_key = :target_facet_key
 ORDER BY origin_doc_id, origin_facet_key
