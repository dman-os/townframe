-- name: insert_pending_member :exec
-- param: scope_id i64
-- param: obj_ref i64
-- param: part_ref i64
INSERT OR IGNORE INTO big_sync_pending_members (scope_id, obj_ref, part_ref)
VALUES (:scope_id, :obj_ref, :part_ref);
