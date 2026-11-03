-- name: delete_owner :exec
-- param: owner String
DELETE FROM big_blobs_byte_owners WHERE owner = :owner;
