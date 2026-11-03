-- name: delete_claim :exec
-- param: inventory_id String
-- param: owner String
DELETE FROM big_blobs_mirror_claims
 WHERE inventory_id = :inventory_id
   AND owner = :owner
