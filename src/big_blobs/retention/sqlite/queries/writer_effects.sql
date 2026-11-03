-- name: writer_effects :many
-- returns: inventory_id String, digest String
SELECT inventory_id, digest FROM big_blobs_writer_effects ORDER BY inventory_id, digest;
