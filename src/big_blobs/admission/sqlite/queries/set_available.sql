-- name: set_available :exec
-- param: bytes bool
-- param: inventory_id String
-- param: digest String
-- param: expected_bytes bool
UPDATE big_blobs_admission_pairs
   SET bytes = :bytes
     , dirty = 1
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND bytes != :expected_bytes
