-- name: bucket_changed_at :optional
-- param: part_key Vec<u8>
-- param: buck_id i64
-- returns: changed_at i64
SELECT changed_at
  FROM big_automata_buckets
 WHERE part_key = :part_key
   AND buck_id = :buck_id
