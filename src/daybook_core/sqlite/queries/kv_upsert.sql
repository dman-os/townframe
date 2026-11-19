-- name: kv_upsert :exec
-- param: key String
-- param: value String
INSERT INTO kvstore(key, value) VALUES (:key, :value)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
