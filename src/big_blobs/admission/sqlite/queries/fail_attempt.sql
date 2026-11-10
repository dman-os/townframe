-- name: fail_attempt :exec
-- param: inventory_id String
-- param: digest String
-- param: generation i64
-- param: attempt i64
UPDATE big_blobs_admission_pairs
   SET generation = generation + 1
     , attempt = NULL
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND generation = :generation
   AND attempt = :attempt
