-- name: complete_failure :exec
-- param: inventory_id String
-- param: digest String
-- param: attempt i64
UPDATE big_blobs_admission_pairs
   SET generation = generation + 1
     , attempt = NULL
     , dirty = 1
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND attempt = :attempt
