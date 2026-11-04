-- name: insert_claim :exec
-- param: inventory_id String
-- param: owner String
INSERT INTO big_blobs_mirror_claims (inventory_id, owner)
VALUES (:inventory_id, :owner)
