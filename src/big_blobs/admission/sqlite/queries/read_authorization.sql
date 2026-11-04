-- name: read_authorization :many
-- param: inventory_id String
-- returns: readable bool
SELECT readable
  FROM big_blobs_inventory_registry
 WHERE inventory_id = :inventory_id
