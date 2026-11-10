-- name: delete_virtual :exec
-- param: cipher_hash String
DELETE FROM big_blobs_virtual_pairs WHERE cipher_hash = :cipher_hash;
