-- name: put_task_update :exec
-- param: sequence i64
-- param: task_id String
-- param: at_unix_secs i64
-- param: update_json String
INSERT OR REPLACE INTO progress_task_updates(sequence, task_id, at_unix_secs, update_json)
VALUES (:sequence, :task_id, :at_unix_secs, :update_json)
