-- name: frontier_member :optional
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- param: maybe_part_ref i64
-- returns: event_type i64, txid i64, payload_json Option<String>
SELECT m.event_type
     , m.txid
     , o.payload_json
  FROM big_sync_members m
  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
 WHERE m.scope_id = :scope_id
   AND m.obj_ref = (SELECT obj_ref FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id)
   AND m.maybe_part_ref = :maybe_part_ref
