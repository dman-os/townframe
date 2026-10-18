-- name: delete_part_member :exec
-- param: scope_id i64
-- param: part_ref i64
-- param: principal_id Vec<u8>
DELETE FROM big_sync_syncable WHERE scope_id = :scope_id AND part_ref = :part_ref AND principal_id = :principal_id;
