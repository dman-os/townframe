-- name: begin_attempt :exec
-- param: inventory_id String
-- param: digest String
UPDATE big_blobs_admission_pairs
   SET attempt = generation
 WHERE inventory_id = :inventory_id
   AND digest = :digest
