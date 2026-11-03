-- name: insert_producer_claim :exec
-- param: inventory_id String
-- param: digest String
-- param: owner String
INSERT OR IGNORE INTO big_blobs_producer_claims (inventory_id, digest, owner)
VALUES (:inventory_id, :digest, :owner)
