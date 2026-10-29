-- name: set_command_timestamps :exec
-- param: created_at i64
-- param: done_at Option<i64>
-- param: command_id String
UPDATE big_cabinet_commands
   SET created_at = :created_at
     , done_at = :done_at
 WHERE command_id = :command_id;
