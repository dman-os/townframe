-- name: pinned :many
-- param: digest String
-- returns: present i64
SELECT 1 AS present
  FROM big_blobs_byte_owners
 WHERE digest = :digest
   AND (owner GLOB 'document:retention:*' OR owner GLOB 'writer:retention:*')
 LIMIT 1;
