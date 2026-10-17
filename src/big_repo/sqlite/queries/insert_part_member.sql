-- name: insert_part_member :exec
-- param: scope_id i64
-- param: part_ref i64
-- param: principal_id Vec<u8>
-- param: access_level i64
-- param: changed_at i64
INSERT INTO big_sync_syncable (scope_id, part_ref, principal_id, access_level, changed_at)
VALUES (:scope_id, :part_ref, :principal_id, :access_level, :changed_at);
