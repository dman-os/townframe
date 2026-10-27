-- name: command_done_at :optional
-- param: command_id String
-- returns: done_at Option<i64>
SELECT done_at
  FROM big_cabinet_commands
 WHERE command_id = :command_id;
