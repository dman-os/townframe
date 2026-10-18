-- name: live_member_parts :many
-- param: scope_id i64
-- param: obj_ref i64
-- param: removed i64
-- returns: maybe_part_ref i64, part_id Vec<u8>
SELECT m.maybe_part_ref
     , p.part_id
  FROM big_sync_members m JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
 WHERE m.scope_id = :scope_id AND m.obj_ref = :obj_ref AND m.maybe_part_ref > 0 AND m.event_type != :removed;
