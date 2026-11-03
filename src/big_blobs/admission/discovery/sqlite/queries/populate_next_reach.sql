-- name: populate_next_reach :exec
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
