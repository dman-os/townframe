-- name: reconcile_reach :exec
DELETE FROM big_blobs_discovery_next_reach;
INSERT INTO big_blobs_discovery_next_reach

WITH RECURSIVE reach (root_id, drawer_id, meta_root, name) AS (
    SELECT p.drawer_id
         , p.drawer_id
         , COALESCE(o.meta_root, p.meta_root)
         , COALESCE(o.name, p.name)
      FROM big_blobs_discovery_policies p
      LEFT JOIN big_blobs_discovery_policies o
        ON o.drawer_id = p.drawer_id AND o.policy = 'override'
     WHERE p.policy = 'root'
    UNION
    SELECT r.root_id
         , e.child_id
         , COALESCE(o.meta_root, r.meta_root)
         , COALESCE(o.name, r.name)
      FROM reach r
      JOIN big_blobs_discovery_documents d ON d.drawer_id = r.drawer_id
      JOIN big_blobs_discovery_edges e ON e.parent_id = r.drawer_id
      LEFT JOIN big_blobs_discovery_policies o
        ON o.drawer_id = e.child_id AND o.policy = 'override'
     WHERE d.self_doc = 0
)
SELECT * FROM reach;
UPDATE big_blobs_discovery_documents
   SET generation = generation + 1
     , attempt = NULL
     , waiting = 1
     , dirty = EXISTS (SELECT 1 FROM big_blobs_discovery_next_reach n
                        WHERE n.drawer_id = big_blobs_discovery_documents.drawer_id)
 WHERE drawer_id IN (
       SELECT n.drawer_id FROM big_blobs_discovery_next_reach n
        WHERE NOT EXISTS (SELECT 1 FROM big_blobs_discovery_reach r
                           WHERE (r.root_id, r.drawer_id, r.meta_root, r.name)
                               = (n.root_id, n.drawer_id, n.meta_root, n.name))
       UNION
       SELECT r.drawer_id FROM big_blobs_discovery_reach r
        WHERE NOT EXISTS (SELECT 1 FROM big_blobs_discovery_next_reach n
                           WHERE (r.root_id, r.drawer_id, r.meta_root, r.name)
                               = (n.root_id, n.drawer_id, n.meta_root, n.name))
 );
DELETE FROM big_blobs_discovery_reach
 WHERE (root_id, drawer_id, meta_root, name) NOT IN (SELECT * FROM big_blobs_discovery_next_reach);
INSERT INTO big_blobs_discovery_reach
SELECT * FROM big_blobs_discovery_next_reach
EXCEPT SELECT * FROM big_blobs_discovery_reach;
DELETE FROM big_blobs_discovery_next_reach;
