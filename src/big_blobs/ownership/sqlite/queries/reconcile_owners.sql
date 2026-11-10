-- name: reconcile_owners :many
-- param: digest String
-- param: prefix String
-- returns: owner String
SELECT owner
  FROM big_blobs_byte_owners
 WHERE digest = :digest
   AND substr(owner, 1, length(:prefix)) = :prefix;
