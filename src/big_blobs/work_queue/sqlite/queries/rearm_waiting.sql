-- name: rearm_waiting :exec
UPDATE big_blobs_work
   SET generation = generation + 1
     , dirty = 1
 WHERE waiting = 1;
