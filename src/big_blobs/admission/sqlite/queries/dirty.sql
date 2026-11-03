-- name: dirty :many
-- returns: inventory_id String, digest String, generation i64
SELECT inventory_id
     , digest
     , generation
  FROM big_blobs_admission_pairs
 WHERE dirty = 1
 ORDER BY inventory_id, digest
 LIMIT 128;
