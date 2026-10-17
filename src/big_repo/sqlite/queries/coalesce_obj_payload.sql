-- name: coalesce_obj_payload :exec
-- param: payload_json Option<String>
-- param: obj_ref i64
UPDATE big_sync_objs SET payload_json = COALESCE(payload_json, :payload_json) WHERE obj_ref = :obj_ref;
