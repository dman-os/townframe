-- name: read_pin_state :many
-- param: inventory_id String
-- returns: pin_revision i64, pins_complete bool
SELECT pin_revision
     , pins_complete
  FROM big_blobs_inventory_registry
 WHERE inventory_id = :inventory_id
