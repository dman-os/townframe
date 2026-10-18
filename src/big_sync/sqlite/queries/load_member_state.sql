-- name: load_member_state :optional
-- param: scope_id i64
-- param: part_id Vec<u8>
-- param: obj_id Vec<u8>
-- returns: event_type i64, payload_json Option<String>
SELECT members.event_type
     , objs.payload_json
  FROM big_sync_members members
  LEFT JOIN big_sync_objs objs ON objs.obj_ref = members.obj_ref
 WHERE members.scope_id = :scope_id
   AND members.maybe_part_ref = (
       SELECT part_ref FROM big_sync_parts
        WHERE scope_id = :scope_id AND part_id = :part_id
   )
   AND members.obj_ref = (
       SELECT obj_ref FROM big_sync_objs
        WHERE scope_id = :scope_id AND obj_id = :obj_id
   )
