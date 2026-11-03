-- name: mark_observed :exec
-- param: self_doc bool
-- param: drawer_id String
UPDATE big_blobs_discovery_documents
   SET observed = 1
     , self_doc = :self_doc
 WHERE drawer_id = :drawer_id;
