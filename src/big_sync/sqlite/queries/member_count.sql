-- name: member_count :optional
-- param: scope_id i64
-- param: part_id Vec<u8>
-- returns: live_count i64
SELECT live_count
  FROM big_sync_buckets
 WHERE scope_id = :scope_id AND part_ref = (
       SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id
       ) AND level = 0 AND buck_id = 0
