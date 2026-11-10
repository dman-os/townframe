-- name: delete_owner_digest :exec
-- param: owner String
-- param: digest String
DELETE FROM big_blobs_byte_owners
 WHERE owner = :owner
   AND digest = :digest;
