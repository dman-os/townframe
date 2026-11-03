-- name: delete_mirror_claims :exec
DELETE FROM big_blobs_mirror_claims
 WHERE (inventory_id, owner) IN (
       SELECT inventory_id, owner FROM big_blobs_discovery_claims
       EXCEPT
       SELECT inventory_id, owner FROM big_blobs_discovery_next_claims
 );
