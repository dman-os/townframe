-- name: populate_next_claims :exec
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
