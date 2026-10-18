-- name: advance_part_cursor :exec
-- param: cursor i64
-- param: scope_id i64
-- param: part_ref i64
UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, :cursor)
 WHERE scope_id = :scope_id AND part_ref = :part_ref;
