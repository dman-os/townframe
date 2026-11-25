-- name: bucket_list :many
-- param: part_key Vec<u8>
-- param: from_id i64
-- param: last_id i64
-- param: since i64
-- param: limit i64
-- returns: buck_id i64, changed_at i64, live_count i64, dead_count i64, live_fp i64, dead_fp i64
SELECT buck_id
     , changed_at
     , live_count
     , dead_count
     , live_fp
     , dead_fp
  FROM big_automata_buckets
 WHERE part_key = :part_key
   AND buck_id BETWEEN :from_id AND :last_id
   AND changed_at > :since
   AND live_count + dead_count > 0
 ORDER BY buck_id
 LIMIT :limit
