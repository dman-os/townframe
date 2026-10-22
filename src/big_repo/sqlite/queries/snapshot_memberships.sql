-- name: snapshot_memberships :many
-- param: scope_id i64
-- returns: part_id Vec<u8>, obj_id Vec<u8>, event_type i64, txid i64
SELECT p.part_id
     , o.obj_id
     , m.event_type
     , m.txid
  FROM big_sync_members m
  JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
 WHERE m.scope_id = :scope_id AND m.maybe_part_ref > 0
 ORDER BY p.part_id, o.obj_id;
