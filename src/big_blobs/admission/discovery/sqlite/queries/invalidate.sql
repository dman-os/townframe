-- name: invalidate :exec
-- param: documents String
UPDATE big_blobs_discovery_documents
   SET generation = generation + 1
     , dirty = 1
     , waiting = 1
     , attempt = NULL
 WHERE dirty = 0
   AND drawer_id IN (SELECT value FROM json_each(:documents))
   AND EXISTS (SELECT 1 FROM big_blobs_discovery_reach r
                WHERE r.drawer_id = big_blobs_discovery_documents.drawer_id);
