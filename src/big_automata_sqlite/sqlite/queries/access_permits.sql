-- name: access_permits :one
-- param: part_key Vec<u8>
-- param: principal Vec<u8>
-- returns: count i64
SELECT COUNT(*) AS count
  FROM big_automata_access
 WHERE part_key = :part_key
   AND principal = :principal
   AND level IS NOT NULL
