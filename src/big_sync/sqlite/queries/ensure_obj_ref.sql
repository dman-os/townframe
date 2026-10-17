-- name: ensure_obj_ref :one
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- param: buck_index i64
-- returns: obj_ref i64
INSERT INTO big_sync_objs(scope_id, obj_id, buck_index)
VALUES (:scope_id, :obj_id, :buck_index)
ON CONFLICT(scope_id, obj_id) DO UPDATE SET obj_id = excluded.obj_id
                                        , buck_index = excluded.buck_index
RETURNING obj_ref
