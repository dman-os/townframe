-- name: delete_root :exec
-- param: cipher_hash String
DELETE FROM blob_pair_root WHERE cipher_hash = :cipher_hash;
