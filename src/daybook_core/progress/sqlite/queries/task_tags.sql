-- name: task_tags :many
-- param: task_id String
-- returns: tag_path String
SELECT tag_path FROM progress_task_tags WHERE task_id = :task_id ORDER BY tag_path ASC
