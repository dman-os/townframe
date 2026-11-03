-- name: length :optional
-- param: inventory_id String
-- param: digest String
-- returns: length_octets i64
SELECT length_octets
  FROM big_blobs_admission_pairs
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND length_octets IS NOT NULL;
