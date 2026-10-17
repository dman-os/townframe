-- name: set_obj_payload :exec
-- param: payload_json String
-- param: obj_ref i64
UPDATE big_sync_objs SET payload_json = :payload_json WHERE obj_ref = :obj_ref
