-- name: access_changes :one
-- param: scope_id i64
-- param: part_id Vec<u8>
-- param: principal_id Vec<u8>
-- param: since i64
-- returns: count i64
SELECT COUNT(*) AS count
  FROM big_sync_syncable
 WHERE scope_id = :scope_id
   AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id)
   AND principal_id = :principal_id
   AND changed_at > :since;
