-- name: pair_root_clear :exec
-- param: cipher_hash String
DELETE FROM blob_pair_root WHERE cipher_hash = :cipher_hash
