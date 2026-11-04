-- name: mark_work :exec
-- param: drawer_id String
UPDATE big_blobs_discovery_documents
   SET dirty = 0
     , attempt = generation
 WHERE drawer_id = :drawer_id;
