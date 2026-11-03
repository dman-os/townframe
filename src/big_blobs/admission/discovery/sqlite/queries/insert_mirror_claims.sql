-- name: insert_mirror_claims :exec
INSERT INTO big_blobs_mirror_claims (inventory_id, owner)
SELECT inventory_id, owner FROM big_blobs_discovery_next_claims
EXCEPT SELECT inventory_id, owner FROM big_blobs_discovery_claims;
