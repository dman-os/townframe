-- name: insert_effect :exec
-- param: digest String
INSERT OR IGNORE INTO big_blobs_byte_effects (digest)
VALUES (:digest);
