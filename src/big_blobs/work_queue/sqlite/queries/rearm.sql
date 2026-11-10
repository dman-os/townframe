-- name: rearm :exec
UPDATE big_blobs_work
   SET generation = generation + 1;
