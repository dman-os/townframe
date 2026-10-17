-- name: obj_payload :optional
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- returns: payload_json Option<String>
SELECT payload_json FROM big_sync_objs WHERE scope_id = :scope_id AND obj_id = :obj_id;
