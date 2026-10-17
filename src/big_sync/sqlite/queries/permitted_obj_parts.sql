-- name: permitted_obj_parts :many
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- param: principal_id Vec<u8>
-- returns: part_id Vec<u8>, access_level i64
SELECT p.part_id
     , s.access_level
  FROM big_sync_members m
  JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
  JOIN big_sync_syncable s ON s.part_ref = m.maybe_part_ref
 WHERE m.scope_id = :scope_id
   AND m.obj_ref = (
       SELECT obj_ref FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id
   )
   AND m.maybe_part_ref > 0
   AND m.event_type != 2
   AND s.scope_id = :scope_id
   AND s.principal_id = :principal_id
 ORDER BY p.part_id
