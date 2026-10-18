-- name: pair_root_record :exec
-- param: cipher_hash String
INSERT OR IGNORE INTO blob_pair_root (cipher_hash) VALUES (:cipher_hash)
