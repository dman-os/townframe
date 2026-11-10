-- name: scan_range :many
-- param: lower Vec<u8>
-- param: upper Vec<u8>
-- returns: key Vec<u8>, value Vec<u8>
SELECT key
     , value
  FROM clay_store_entry
 WHERE key >= :lower
   AND key < :upper
 ORDER BY key
