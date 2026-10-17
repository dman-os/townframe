-- name: pending_parts :many
-- param: scope_id i64
-- param: obj_ref i64
-- returns: part_ref i64, part_id Vec<u8>
SELECT p.part_ref
     , p.part_id
  FROM big_sync_pending_members m
  JOIN big_sync_parts p ON p.part_ref = m.part_ref
 WHERE m.scope_id = :scope_id AND m.obj_ref = :obj_ref
