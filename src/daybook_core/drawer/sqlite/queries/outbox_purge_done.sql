-- name: outbox_purge_done :exec
-- param: cutoff i64
DELETE FROM drawer_add_outbox WHERE state = 'done' AND done_at IS NOT NULL AND done_at < :cutoff
