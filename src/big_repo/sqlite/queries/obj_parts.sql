-- name: obj_parts :many
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- param: removed i64
-- returns: part_id Vec<u8>
SELECT p.part_id
  FROM big_sync_members m
  JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
 WHERE m.scope_id = :scope_id
   AND m.obj_ref = (SELECT obj_ref FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id)
   AND m.maybe_part_ref > 0 AND m.event_type != :removed
 UNION
SELECT p.part_id
  FROM big_sync_pending_members m
  JOIN big_sync_parts p ON p.part_ref = m.part_ref
 WHERE m.scope_id = :scope_id
   AND m.obj_ref = (SELECT obj_ref FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id)
 ORDER BY part_id ASC;
