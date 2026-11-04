-- name: mark_reach_changed :exec
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
