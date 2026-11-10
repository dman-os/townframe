-- name: insert_effect_plaintext :exec
-- param: digest String
INSERT OR IGNORE INTO big_blobs_byte_effects (digest)
SELECT plaintext
  FROM big_blobs_byte_dependencies
 WHERE digest = :digest;
