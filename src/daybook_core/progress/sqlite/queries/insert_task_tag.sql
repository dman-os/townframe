-- name: insert_task_tag :exec
-- param: task_id String
-- param: tag_path String
INSERT INTO progress_task_tags(task_id, tag_path) VALUES (:task_id, :tag_path)
ON CONFLICT(task_id, tag_path) DO NOTHING
