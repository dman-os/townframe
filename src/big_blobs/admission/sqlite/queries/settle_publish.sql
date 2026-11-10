-- name: settle_publish :exec
-- param: bytes bool
-- param: inventory_id String
-- param: digest String
-- param: generation i64
UPDATE big_blobs_admission_pairs
   SET bytes = :bytes
     , dirty = 0
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND generation = :generation
   AND dirty = 1
