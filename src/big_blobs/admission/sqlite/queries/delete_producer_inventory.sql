-- name: delete_producer_inventory :exec
-- param: inventory_id String
DELETE FROM big_blobs_producer_claims
 WHERE inventory_id = :inventory_id
