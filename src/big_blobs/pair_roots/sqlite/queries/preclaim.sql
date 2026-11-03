-- name: preclaim :many
-- param: cipher_hash String
-- returns: present i64
SELECT 1 AS present
  FROM blob_pair_root
 WHERE cipher_hash = :cipher_hash;
