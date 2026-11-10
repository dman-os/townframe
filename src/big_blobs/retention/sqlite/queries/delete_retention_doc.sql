-- name: delete_retention_doc :exec
-- param: doc_id String
DELETE FROM big_blobs_retention WHERE doc_id = :doc_id;
