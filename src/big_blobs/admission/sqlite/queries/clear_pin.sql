-- name: clear_pin :exec
-- param: inventory_id String
-- param: digest String
UPDATE big_blobs_admission_pairs
   SET length_octets = NULL
     , generation = generation + 1
     , attempt = NULL
     , dirty = 1
 WHERE inventory_id = :inventory_id
   AND digest = :digest
