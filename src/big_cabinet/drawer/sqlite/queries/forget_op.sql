-- name: forget_op :exec
-- param: command_id String
DELETE FROM big_cabinet_drawer_ops
 WHERE command_id = :command_id;
