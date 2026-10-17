-- name: bucket_summary_for_path :optional
-- param: scope_id i64
-- param: part_id Vec<u8>
-- param: level i64
-- param: buck_id i64
-- returns: changed_at i64, live_count i64, dead_count i64, live_fp i64, dead_fp i64
SELECT changed_at
     , live_count
     , dead_count
     , live_fp
     , dead_fp
  FROM big_sync_buckets
 WHERE scope_id = :scope_id AND part_ref = (
       SELECT part_ref FROM big_sync_parts
        WHERE scope_id = :scope_id AND part_id = :part_id
       ) AND level = :level AND buck_id = :buck_id
