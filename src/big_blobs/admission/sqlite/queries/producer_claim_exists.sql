-- name: producer_claim_exists :many
-- param: inventory_id String
-- param: digest String
-- param: owner String
-- returns: value i64
SELECT 1 AS value
  FROM big_blobs_producer_claims
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND owner = :owner
