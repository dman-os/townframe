-- name: latest_revision :one
-- returns: value i64
SELECT value FROM big_sync_meta WHERE key = 'global_cursor';
