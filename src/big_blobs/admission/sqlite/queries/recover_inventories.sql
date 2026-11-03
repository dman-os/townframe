-- name: recover_inventories :exec
UPDATE big_blobs_inventory_registry
   SET readable = 0
     , authorization = authorization + 1
