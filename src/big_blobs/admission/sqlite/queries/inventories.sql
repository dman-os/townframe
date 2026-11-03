-- name: inventories :many
-- returns: inventory_id String
SELECT inventory_id
  FROM big_blobs_inventory_registry
 ORDER BY inventory_id;
