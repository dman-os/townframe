-- name: writer_effects_from_claims :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
INSERT OR IGNORE INTO big_blobs_writer_effects (inventory_id, digest)
SELECT inventory_id
     , digest
  FROM big_blobs_writer_claims
 WHERE doc_id = :doc_id
   AND drawer_id = :drawer_id
   AND root = :root
   AND name = :name;
