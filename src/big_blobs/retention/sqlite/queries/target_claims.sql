-- name: target_claims :many
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
-- returns: digest String, length_octets i64
SELECT digest
     , length_octets
  FROM big_blobs_writer_claims
 WHERE doc_id = :doc_id
   AND drawer_id = :drawer_id
   AND root = :root
   AND name = :name
 ORDER BY digest;
