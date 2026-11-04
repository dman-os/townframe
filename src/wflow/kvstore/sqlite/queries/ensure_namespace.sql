-- name: ensure_namespace :one
-- param: name String
-- returns: namespace_id i64
INSERT INTO wflow_atomic_namespaces(name, revision)
VALUES (:name, 0)
ON CONFLICT(name) DO UPDATE SET name = excluded.name
RETURNING namespace_id
