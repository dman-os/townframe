-- name: recover_pairs :exec
UPDATE big_blobs_admission_pairs
   SET generation = generation + 1
     , attempt = NULL
     , dirty = 1
