-- name: update_authorization :exec
-- param: readable bool
-- param: inventory_id String
UPDATE big_blobs_inventory_registry
   SET readable = :readable
     , authorization = authorization + 1
 WHERE inventory_id = :inventory_id
