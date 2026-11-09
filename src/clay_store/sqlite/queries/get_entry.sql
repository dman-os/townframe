-- name: get_entry :optional
-- param: key Vec<u8>
-- returns: value Vec<u8>
SELECT value
  FROM clay_store_entry
 WHERE key = :key
