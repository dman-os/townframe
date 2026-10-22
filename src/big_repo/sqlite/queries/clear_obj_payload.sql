-- name: clear_obj_payload :exec
-- param: obj_ref i64
UPDATE big_sync_objs SET payload_json = NULL WHERE obj_ref = :obj_ref;
