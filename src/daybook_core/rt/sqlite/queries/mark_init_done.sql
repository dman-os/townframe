-- name: mark_init_done :exec
-- param: init_id String
-- param: completed_at String
INSERT INTO init_per_node(init_id, completed_at)
VALUES (:init_id, :completed_at)
ON CONFLICT(init_id) DO UPDATE SET completed_at = excluded.completed_at
