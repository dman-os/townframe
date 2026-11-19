-- name: outgoing_edges :many
-- param: origin_doc_id String
-- into: crate::index::facet_ref::EdgeRow
-- returns: origin_doc_id String, origin_facet_key String, target_doc_id String, target_facet_key String, reference_kind String, origin_heads String
SELECT origin_doc_id
     , origin_facet_key
     , target_doc_id
     , target_facet_key
     , reference_kind
     , origin_heads
  FROM facet_ref_edges
 WHERE origin_doc_id = :origin_doc_id
 ORDER BY origin_facet_key, target_doc_id, target_facet_key
