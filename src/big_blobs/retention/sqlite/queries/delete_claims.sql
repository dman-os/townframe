-- name: delete_claims :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
DELETE FROM big_blobs_writer_claims
 WHERE doc_id = :doc_id AND drawer_id = :drawer_id AND root = :root AND name = :name;
