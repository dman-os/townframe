-- name: writer_effects_by_digest :many
-- param: digest String
-- returns: inventory_id String
SELECT inventory_id
  FROM big_blobs_writer_effects
 WHERE digest = :digest;
