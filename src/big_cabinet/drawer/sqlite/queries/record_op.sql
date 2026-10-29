-- name: record_op :exec
-- param: op_json String
-- param: command_id String
UPDATE big_cabinet_drawer_ops
   SET op_json = :op_json
 WHERE command_id = :command_id;
