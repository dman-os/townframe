-- name: delete_stale_claims :exec
DELETE FROM big_blobs_discovery_claims
 WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_discovery_next_claims);
