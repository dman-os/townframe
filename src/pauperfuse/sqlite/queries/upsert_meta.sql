-- name: upsert_meta :exec
-- param: key String
-- param: value String
INSERT INTO pauperfuse_meta (key, value) VALUES (:key, :value)
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
