-- name: prior_agent_ids :many
-- param: scope_id i64
-- param: obj_ref i64
-- returns: principal_id Vec<u8>
SELECT DISTINCT s.principal_id FROM big_sync_syncable s
 WHERE s.scope_id = :scope_id
   AND s.part_ref IN (
       SELECT m.maybe_part_ref FROM big_sync_members m
        WHERE m.scope_id = :scope_id AND m.obj_ref = :obj_ref AND m.maybe_part_ref > 0 AND m.event_type != 2
   );
