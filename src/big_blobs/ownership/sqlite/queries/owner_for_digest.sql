-- name: owner_for_digest :many
-- param: digest String
-- returns: owner String
SELECT owner
  FROM big_blobs_byte_owners
 WHERE digest = :digest;
