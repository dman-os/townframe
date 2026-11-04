-- name: mark_claim_pairs :exec
UPDATE big_blobs_admission_pairs
   SET generation = generation + 1
     , attempt = NULL
     , dirty = 1
 WHERE inventory_id IN (
       SELECT inventory_id FROM big_blobs_discovery_claims
        WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_discovery_next_claims)
       UNION
       SELECT inventory_id FROM big_blobs_discovery_next_claims
        WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_discovery_claims)
 );
