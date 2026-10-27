-- name: doc_blob_hashes :many
-- param: doc_id String
-- returns: blob_hash String
SELECT DISTINCT blob_hash
  FROM doc_blob_pins
 WHERE doc_id = :doc_id
 ORDER BY blob_hash ASC
