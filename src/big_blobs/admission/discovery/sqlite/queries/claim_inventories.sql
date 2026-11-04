-- name: claim_inventories :many
-- returns: inventory_id String
SELECT DISTINCT c.inventory_id
  FROM big_blobs_discovery_claims c
  LEFT JOIN big_blobs_inventory_registry r USING (inventory_id)
 WHERE r.inventory_id IS NULL;
