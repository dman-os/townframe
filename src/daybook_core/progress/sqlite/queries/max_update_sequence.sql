-- name: max_update_sequence :one
-- returns: sequence Option<i64>
SELECT MAX(sequence) AS sequence FROM progress_task_updates
