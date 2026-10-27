-- name: delete_pending_member :exec
-- param: scope_id i64
-- param: obj_ref i64
-- param: part_ref i64
DELETE FROM big_sync_pending_members
 WHERE scope_id = :scope_id AND obj_ref = :obj_ref AND part_ref = :part_ref
