-- name: obj_payload_by_ref :optional
-- param: obj_ref i64
-- returns: payload_json Option<String>
SELECT payload_json FROM big_sync_objs WHERE obj_ref = :obj_ref;
