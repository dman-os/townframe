-- name: rearm_inventory :exec
-- param: inventory_id String
UPDATE big_blobs_admission_pairs
   SET generation = generation + 1
     , attempt = NULL
     , dirty = 1
 WHERE inventory_id = :inventory_id
