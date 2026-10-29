-- name: begin_op :exec
-- param: command_id String
-- param: request_digest Vec<u8>
-- param: actor_uuid String
-- param: op_json String
-- param: created_at i64
INSERT INTO big_cabinet_drawer_ops
     ( command_id
     , request_digest
     , actor_uuid
     , op_json
     , step
     , created_at
     )
VALUES (:command_id, :request_digest, :actor_uuid, :op_json, 'begun', :created_at)
ON CONFLICT (command_id) DO NOTHING;
