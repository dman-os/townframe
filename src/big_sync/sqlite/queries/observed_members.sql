-- name: observed_members :many
-- param: scope_id i64
-- param: removed i64
-- returns: obj_id Vec<u8>, part_id Vec<u8>, payload_json Option<String>
SELECT objs.obj_id
     , parts.part_id
     , objs.payload_json
  FROM big_sync_members members
  JOIN big_sync_objs objs ON objs.obj_ref = members.obj_ref
  JOIN big_sync_parts parts ON parts.part_ref = members.maybe_part_ref
 WHERE members.scope_id = :scope_id AND members.maybe_part_ref > 0
   AND members.event_type != :removed
 ORDER BY objs.obj_id ASC, parts.part_id ASC
