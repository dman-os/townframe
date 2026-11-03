-- name: contains :many
-- param: digest String
-- returns: present i64
SELECT 1 AS present
  FROM big_blobs_byte_owners
 WHERE digest = :digest
 UNION ALL
SELECT 1 AS present
  FROM big_blobs_byte_dependencies AS dependency
  JOIN big_blobs_byte_owners AS owner ON owner.digest = dependency.digest
 WHERE dependency.plaintext = :digest
 LIMIT 1;
