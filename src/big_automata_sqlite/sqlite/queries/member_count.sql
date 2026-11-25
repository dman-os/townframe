-- name: member_count :optional
-- param: part_key Vec<u8>
-- returns: live_count i64
SELECT live_count
  FROM big_automata_buckets
 WHERE part_key = :part_key
   AND buck_id = 0
