-- name: delete_task_tags :exec
-- param: task_id String
DELETE FROM progress_task_tags WHERE task_id = :task_id
