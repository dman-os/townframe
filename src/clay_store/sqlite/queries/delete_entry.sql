-- name: delete_entry :exec
-- param: key Vec<u8>
DELETE FROM clay_store_entry
 WHERE key = :key
