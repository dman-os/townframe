-- name: inventory_gaps :many
-- param: root_id String
-- returns: inventory_id String
-- returns: readable bool
-- returns: pins_complete bool
SELECT DISTINCT r.inventory_id
     , r.readable
     , r.pins_complete
  FROM big_blobs_discovery_claims c
  JOIN big_blobs_inventory_registry r USING (inventory_id)
 WHERE c.root_id = :root_id
 ORDER BY r.inventory_id;
