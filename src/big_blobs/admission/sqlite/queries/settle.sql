-- name: settle :exec
-- param: inventory_id String
-- param: digest String
-- param: generation i64
UPDATE big_blobs_admission_pairs
   SET dirty = 0
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND generation = :generation
   AND dirty = 1
