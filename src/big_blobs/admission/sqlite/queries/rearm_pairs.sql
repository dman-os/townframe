-- name: rearm_pairs :exec
UPDATE big_blobs_admission_pairs
   SET dirty = 1
 WHERE dirty = 0
