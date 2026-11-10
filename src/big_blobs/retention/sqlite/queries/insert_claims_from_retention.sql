-- name: insert_claims_from_retention :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
-- param: inventory_id String
INSERT INTO big_blobs_writer_claims
    (doc_id, drawer_id, root, name, inventory_id, digest, length_octets)
SELECT :doc_id
     , :drawer_id
     , :root
     , :name
     , :inventory_id
     , digest
     , length_octets
  FROM big_blobs_retention
 WHERE doc_id = :doc_id
   AND root = :root;
