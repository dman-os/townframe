-- name: delete_doc_edges :exec
-- param: doc_id String
DELETE FROM facet_ref_edges WHERE origin_doc_id = :doc_id OR target_doc_id = :doc_id
