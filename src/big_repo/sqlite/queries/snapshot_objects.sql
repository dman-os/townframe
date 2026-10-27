-- name: snapshot_objects :many
-- param: scope_id i64
-- returns: obj_id Vec<u8>, payload_json Option<String>
SELECT obj_id
     , payload_json
  FROM big_sync_objs WHERE scope_id = :scope_id ORDER BY obj_id;
