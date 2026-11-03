-- name: pinned_digests :many
-- param: inventory_id String
-- returns: digest String
SELECT digest
  FROM big_blobs_admission_pairs
 WHERE inventory_id = :inventory_id
   AND length_octets IS NOT NULL
