-- name: advance_command :exec
-- param: state String
-- param: heads_json String
-- param: done_at i64
-- param: command_id String
-- param: from_first String
-- param: from_last String
UPDATE big_cabinet_commands
   SET state = :state
     , heads_json = :heads_json
     , done_at = CASE WHEN :state = 'done' THEN :done_at ELSE done_at END
 WHERE command_id = :command_id
   AND state IN (:from_first, :from_last);
