-- name: delete_from :exec
-- param: lower Vec<u8>
DELETE FROM clay_store_entry
 WHERE key >= :lower
