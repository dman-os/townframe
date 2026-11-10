-- name: retention_for_doc :many
-- param: doc_id String
-- returns: digest String
SELECT digest FROM big_blobs_retention WHERE doc_id = :doc_id;
