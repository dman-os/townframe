-- name: delete_range :exec
-- param: lower Vec<u8>
-- param: upper Vec<u8>
DELETE FROM clay_store_entry
 WHERE key >= :lower
   AND key < :upper
