-- name: touch_pair :exec
-- param: inventory_id String
-- param: digest String
INSERT INTO big_blobs_admission_pairs (inventory_id, digest)
VALUES (:inventory_id, :digest)
ON CONFLICT (inventory_id, digest) DO UPDATE
   SET generation = generation + 1
     , attempt = NULL
     , dirty = 1
