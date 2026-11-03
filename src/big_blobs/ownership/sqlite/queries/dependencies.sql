-- name: dependencies :many
-- param: digest String
-- returns: plaintext String
SELECT plaintext
  FROM big_blobs_byte_dependencies
 WHERE digest = :digest;
