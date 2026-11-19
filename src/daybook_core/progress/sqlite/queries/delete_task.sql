-- name: delete_task :exec
-- param: id String
DELETE FROM progress_tasks WHERE id = :id
