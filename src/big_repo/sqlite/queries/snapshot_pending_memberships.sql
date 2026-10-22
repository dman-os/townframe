-- name: snapshot_pending_memberships :many
-- param: scope_id i64
-- returns: part_id Vec<u8>, obj_id Vec<u8>
SELECT p.part_id
     , o.obj_id
  FROM big_sync_pending_members m
  JOIN big_sync_parts p ON p.part_ref = m.part_ref
  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
 WHERE m.scope_id = :scope_id ORDER BY p.part_id, o.obj_id;
