-- name: part_store_stats :one
-- param: scope_id i64
-- param: removed i64
-- returns: payload_objects i64, payload_bytes i64, partless_objects i64, live_rows i64, dead_rows i64
SELECT (SELECT COUNT(*) FROM big_sync_objs o
         WHERE o.scope_id = :scope_id AND o.payload_json IS NOT NULL AND o.payload_json != '') AS payload_objects
     , (SELECT COALESCE(SUM(length(o.payload_json)), 0) FROM big_sync_objs o
         WHERE o.scope_id = :scope_id AND o.payload_json IS NOT NULL AND o.payload_json != '') AS payload_bytes
     , (SELECT COUNT(*) FROM big_sync_objs o
         WHERE o.scope_id = :scope_id AND o.payload_json IS NOT NULL AND o.payload_json != ''
           AND NOT EXISTS (
               SELECT 1 FROM big_sync_members m
                WHERE m.scope_id = o.scope_id AND m.obj_ref = o.obj_ref
                  AND m.maybe_part_ref > 0 AND m.event_type != :removed
           )) AS partless_objects
     , (SELECT COUNT(*) FROM big_sync_members m
         WHERE m.scope_id = :scope_id AND m.maybe_part_ref > 0 AND m.event_type != :removed) AS live_rows
     , (SELECT COUNT(*) FROM big_sync_members m
         WHERE m.scope_id = :scope_id AND m.maybe_part_ref > 0 AND m.event_type = :removed) AS dead_rows;
