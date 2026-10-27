-- name: upsert_bucket :exec
-- param: scope_id i64
-- param: part_ref i64
-- param: buck_id i64
-- param: level i64
-- param: changed_at i64
-- param: live_count i64
-- param: dead_count i64
-- param: live_fp i64
-- param: dead_fp i64
INSERT INTO big_sync_buckets(
    scope_id
  , part_ref
  , buck_id
  , level
  , changed_at
  , live_count
  , dead_count
  , live_fp
  , dead_fp
) VALUES (:scope_id, :part_ref, :buck_id, :level, :changed_at, :live_count, :dead_count, :live_fp, :dead_fp)
ON CONFLICT(scope_id, part_ref, buck_id) DO UPDATE SET
    level = excluded.level
  , changed_at = excluded.changed_at
  , live_count = excluded.live_count
  , dead_count = excluded.dead_count
  , live_fp = excluded.live_fp
  , dead_fp = excluded.dead_fp
