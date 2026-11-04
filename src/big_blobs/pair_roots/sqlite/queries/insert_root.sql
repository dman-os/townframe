-- name: insert_root :exec
-- param: cipher_hash String
INSERT OR IGNORE INTO blob_pair_root (cipher_hash)
VALUES (:cipher_hash);
