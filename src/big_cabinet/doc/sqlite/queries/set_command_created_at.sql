-- name: set_command_created_at :exec
-- param: created_at i64
-- param: command_id String
UPDATE big_cabinet_commands
   SET created_at = :created_at
 WHERE command_id = :command_id;
