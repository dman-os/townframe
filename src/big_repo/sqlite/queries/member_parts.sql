-- name: member_parts :many
-- param: scope_id i64
-- param: obj_ref i64
-- returns: part_id Vec<u8>, part_ref i64
SELECT p.part_id
     , p.part_ref
  FROM big_sync_parts p
 WHERE p.scope_id = :scope_id
   AND p.part_ref IN (
       SELECT maybe_part_ref FROM big_sync_members
        WHERE scope_id = :scope_id AND obj_ref = :obj_ref AND maybe_part_ref > 0
        UNION
       SELECT part_ref FROM big_sync_pending_members
        WHERE scope_id = :scope_id AND obj_ref = :obj_ref
   );
