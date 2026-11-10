-- name: upsert_pin :exec
-- param: inventory_id String
-- param: digest String
-- param: length_octets i64
INSERT INTO big_blobs_admission_pairs (inventory_id, digest, length_octets)
VALUES (:inventory_id, :digest, :length_octets)
ON CONFLICT (inventory_id, digest) DO UPDATE
   SET length_octets = excluded.length_octets
     , generation = generation + 1
     , attempt = NULL
     , dirty = 1
 WHERE length_octets IS NOT excluded.length_octets
