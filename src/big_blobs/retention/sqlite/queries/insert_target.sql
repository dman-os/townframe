-- name: insert_target :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
-- param: inventory_id String
INSERT INTO big_blobs_writer_targets (doc_id, drawer_id, root, name, inventory_id)
VALUES (:doc_id, :drawer_id, :root, :name, :inventory_id);
