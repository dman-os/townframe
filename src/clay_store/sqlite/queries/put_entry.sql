-- name: put_entry :exec
-- param: key Vec<u8>
-- param: value Vec<u8>
INSERT INTO clay_store_entry (key, value)
VALUES (:key, :value)
ON CONFLICT (key) DO UPDATE SET value = excluded.value
