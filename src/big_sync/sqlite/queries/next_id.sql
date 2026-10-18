-- name: next_id :one
-- param: key String
-- returns: value i64
UPDATE big_sync_meta SET value = value + 1 WHERE key = :key RETURNING value
