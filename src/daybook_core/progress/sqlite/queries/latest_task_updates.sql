-- name: latest_task_updates :many
-- param: task_id String
-- param: limit i64
-- returns: sequence i64, at_unix_secs i64, update_json String
SELECT sequence, at_unix_secs, update_json
  FROM progress_task_updates
 WHERE task_id = :task_id
 ORDER BY sequence DESC
 LIMIT :limit
