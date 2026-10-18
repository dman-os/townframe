-- name: kv_insert :exec
-- param: key String
-- param: value String
INSERT INTO kvstore(key, value) VALUES (:key, :value)
