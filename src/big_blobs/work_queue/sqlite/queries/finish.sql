-- name: finish :exec
-- param: waiting bool
-- param: doc_id String
-- param: generation i64
UPDATE big_blobs_work
   SET dirty = 0
     , waiting = :waiting
 WHERE doc_id = :doc_id
   AND generation = :generation;
