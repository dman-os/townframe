-- name: scan_bounded :many
-- param: namespace_id i64
-- param: lower Vec<u8>
-- param: upper Vec<u8>
-- param: limit i64
-- returns: key Vec<u8>, value Vec<u8>
SELECT key
     , value
  FROM wflow_atomic_entries
 WHERE namespace_id = :namespace_id
   AND key >= :lower
   AND key < :upper
 ORDER BY key
 LIMIT :limit
