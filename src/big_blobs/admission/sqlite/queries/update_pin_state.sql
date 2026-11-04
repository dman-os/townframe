-- name: update_pin_state :exec
-- param: pin_revision i64
-- param: pins_complete bool
-- param: inventory_id String
UPDATE big_blobs_inventory_registry
   SET pin_revision = :pin_revision
     , pins_complete = :pins_complete
 WHERE inventory_id = :inventory_id
