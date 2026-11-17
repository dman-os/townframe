-- name: scan_from :many
-- param: namespace_id i64
-- param: lower Vec<u8>
-- param: limit i64
-- returns: key Vec<u8>, value Vec<u8>
SELECT key
     , value
  FROM wflow_atomic_entries
 WHERE namespace_id = :namespace_id
   AND key >= :lower
 ORDER BY key
 LIMIT :limit
