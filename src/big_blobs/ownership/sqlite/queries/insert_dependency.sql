-- name: insert_dependency :exec
-- param: digest String
-- param: plaintext String
INSERT OR IGNORE INTO big_blobs_byte_dependencies (digest, plaintext)
VALUES (:digest, :plaintext);
