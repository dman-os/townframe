-- name: managed_pin :many
-- param: digest String
-- returns: present i64
SELECT 1 AS present
  FROM big_blobs_byte_pin_roots
 WHERE digest = :digest;
