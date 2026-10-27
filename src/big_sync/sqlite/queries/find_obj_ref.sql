-- name: find_obj_ref :optional
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- returns: obj_ref i64
SELECT obj_ref FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id
