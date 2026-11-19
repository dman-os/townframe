-- name: delete_outgoing_edges :exec
-- param: origin_doc_id String
DELETE FROM facet_ref_edges WHERE origin_doc_id = :origin_doc_id
