-- name: partless_objects :many
-- param: scope_id i64
-- param: after Option<Vec<u8>>
-- param: removed i64
-- param: limit i64
-- returns: obj_id Vec<u8>
SELECT o.obj_id
  FROM big_sync_objs o
 WHERE o.scope_id = :scope_id
   AND o.payload_json IS NOT NULL AND o.payload_json != ''
   AND (:after IS NULL OR o.obj_id > :after)
   AND NOT EXISTS (
       SELECT 1 FROM big_sync_members m
        WHERE m.scope_id = o.scope_id AND m.obj_ref = o.obj_ref
          AND m.maybe_part_ref > 0 AND m.event_type != :removed
   )
 ORDER BY o.obj_id
 LIMIT :limit;
