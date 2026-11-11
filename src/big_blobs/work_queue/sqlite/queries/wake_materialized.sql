-- name: wake_materialized :exec
-- param: doc_id String
UPDATE big_blobs_work
   SET generation = generation + 1
     , dirty = 1
 WHERE doc_id = :doc_id
   AND (waiting = 1 OR dirty = 1);
