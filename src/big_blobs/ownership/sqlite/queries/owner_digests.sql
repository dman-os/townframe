-- name: owner_digests :many
-- param: owner String
-- returns: digest String
SELECT digest
  FROM big_blobs_byte_owners
 WHERE owner = :owner;
