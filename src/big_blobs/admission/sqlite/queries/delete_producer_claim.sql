-- name: delete_producer_claim :exec
-- param: inventory_id String
-- param: digest String
-- param: owner String
DELETE FROM big_blobs_producer_claims
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND owner = :owner
