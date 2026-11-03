-- name: claim_exists :many
-- param: inventory_id String
-- param: owner String
-- returns: value i64
SELECT 1 AS value
  FROM big_blobs_mirror_claims
 WHERE inventory_id = :inventory_id
   AND owner = :owner
