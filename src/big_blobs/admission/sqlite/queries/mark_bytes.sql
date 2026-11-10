-- name: mark_bytes :exec
-- param: inventory_id String
-- param: digest String
UPDATE big_blobs_admission_pairs
   SET bytes = 1
 WHERE inventory_id = :inventory_id
   AND digest = :digest
