-- name: writer_target_by_key :many
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
-- returns: inventory_id String
SELECT inventory_id FROM big_blobs_writer_targets
 WHERE doc_id = :doc_id AND drawer_id = :drawer_id AND root = :root AND name = :name;
