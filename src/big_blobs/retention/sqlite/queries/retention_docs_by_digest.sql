-- name: retention_docs_by_digest :many
-- param: digest String
-- returns: doc_id String
SELECT doc_id
  FROM big_blobs_retention
 WHERE digest = :digest;
