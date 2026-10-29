-- name: finish_op :exec
-- param: receipt_json String
-- param: done_at i64
-- param: command_id String
UPDATE big_cabinet_drawer_ops
   SET step = 'done'
     , receipt_json = :receipt_json
     , done_at = :done_at
 WHERE command_id = :command_id;
