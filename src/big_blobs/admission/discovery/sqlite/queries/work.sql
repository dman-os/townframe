-- name: work :many
-- returns: drawer_id String
-- returns: generation i64
SELECT d.drawer_id
     , d.generation
  FROM big_blobs_discovery_documents d
 WHERE d.dirty = 1
   AND EXISTS (SELECT 1 FROM big_blobs_discovery_reach r WHERE r.drawer_id = d.drawer_id)
 ORDER BY d.drawer_id
 LIMIT 1;
