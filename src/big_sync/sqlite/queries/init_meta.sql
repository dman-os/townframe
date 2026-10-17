-- name: init_meta :exec
-- param: key String
INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES (:key, 0)
