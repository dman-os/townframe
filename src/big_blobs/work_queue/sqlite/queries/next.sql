-- name: next :many
-- returns: doc_id String, generation i64
SELECT doc_id
     , generation
  FROM big_blobs_work
 WHERE dirty = 1
 ORDER BY doc_id
 LIMIT 1;
