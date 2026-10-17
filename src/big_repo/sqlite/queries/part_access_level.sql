-- name: part_access_level :optional
-- param: scope_id i64
-- param: part_ref i64
-- param: principal_id Vec<u8>
-- returns: access_level i64
SELECT access_level FROM big_sync_syncable
 WHERE scope_id = :scope_id AND part_ref = :part_ref AND principal_id = :principal_id;
