-- name: insert_pin_root :exec
-- param: digest String
INSERT OR IGNORE INTO big_blobs_byte_pin_roots (digest)
VALUES (:digest);
