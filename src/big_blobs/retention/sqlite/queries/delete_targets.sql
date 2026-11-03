-- name: delete_targets :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
DELETE FROM big_blobs_writer_targets
 WHERE doc_id = :doc_id AND drawer_id = :drawer_id AND root = :root AND name = :name;
