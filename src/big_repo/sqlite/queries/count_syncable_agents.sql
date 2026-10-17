-- name: count_syncable_agents :one
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- returns: count i64
SELECT COUNT(*) AS count FROM big_sync_syncable s
  JOIN big_sync_members m ON m.maybe_part_ref = s.part_ref
  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
 WHERE s.scope_id = :scope_id AND o.obj_id = :obj_id;
