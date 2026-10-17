-- name: delete_part_members :exec
-- param: scope_id i64
-- param: part_ref i64
DELETE FROM big_sync_syncable WHERE scope_id = :scope_id AND part_ref = :part_ref
