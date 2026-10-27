-- name: get_op :optional
-- param: command_id String
-- returns: command_id String, request_digest Vec<u8>, actor_uuid String, op_json String, step String, receipt_json Option<String>, done_at Option<i64>
SELECT command_id
     , request_digest
     , actor_uuid
     , op_json
     , step
     , receipt_json
     , done_at
  FROM big_cabinet_drawer_ops
 WHERE command_id = :command_id;
