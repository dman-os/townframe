-- name: discard_command :exec
-- param: command_id String
DELETE FROM big_cabinet_commands
 WHERE command_id = :command_id
   AND state != 'done';
