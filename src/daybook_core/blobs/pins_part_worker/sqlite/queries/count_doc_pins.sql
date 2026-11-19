-- name: count_doc_pins :one
-- param: doc_id String
-- returns: count i64
SELECT COUNT(*) AS count FROM doc_blob_pins WHERE doc_id = :doc_id
