-- name: retention_pins :many
-- param: doc_id String
-- param: root String
-- returns: digest String, length_octets i64
SELECT digest
     , length_octets
  FROM big_blobs_retention
 WHERE doc_id = :doc_id
   AND root = :root
 ORDER BY digest;
