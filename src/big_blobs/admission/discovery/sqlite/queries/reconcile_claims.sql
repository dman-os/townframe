-- name: reconcile_claims :exec
DELETE FROM big_blobs_discovery_next_claims;
INSERT INTO big_blobs_discovery_next_claims
SELECT s.inventory_id
     , 'discovery/' || r.root_id || '/' || r.drawer_id || '/' || r.meta_root
       || '/' || hex(CAST(r.name AS BLOB)) || '/' || s.inventory_id AS owner
     , r.root_id
     , r.drawer_id
     , r.meta_root
     , r.name
  FROM big_blobs_discovery_next_reach r
  JOIN big_blobs_discovery_documents d ON d.drawer_id = r.drawer_id
  JOIN big_blobs_discovery_slots s
    ON (s.drawer_id, s.meta_root, s.name) = (r.drawer_id, r.meta_root, r.name)
 WHERE d.self_doc = 0;

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
DELETE FROM big_blobs_mirror_claims
 WHERE (inventory_id, owner) IN (
       SELECT inventory_id, owner FROM big_blobs_discovery_claims
       EXCEPT
       SELECT inventory_id, owner FROM big_blobs_discovery_next_claims
 );
INSERT INTO big_blobs_mirror_claims (inventory_id, owner)
SELECT inventory_id, owner FROM big_blobs_discovery_next_claims
EXCEPT SELECT inventory_id, owner FROM big_blobs_discovery_claims;
DELETE FROM big_blobs_discovery_claims
 WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_discovery_next_claims);
INSERT INTO big_blobs_discovery_claims
SELECT * FROM big_blobs_discovery_next_claims
EXCEPT SELECT * FROM big_blobs_discovery_claims;
DELETE FROM big_blobs_discovery_next_claims;
