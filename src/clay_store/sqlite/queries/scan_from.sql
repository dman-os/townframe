-- name: scan_from :many
-- param: lower Vec<u8>
-- returns: key Vec<u8>, value Vec<u8>
SELECT key
     , value
  FROM clay_store_entry
 WHERE key >= :lower
 ORDER BY key
