-- name: delete_partless_member :exec
-- param: scope_id i64
-- param: obj_ref i64
DELETE FROM big_sync_members WHERE scope_id = :scope_id AND obj_ref = :obj_ref AND maybe_part_ref = 0;
