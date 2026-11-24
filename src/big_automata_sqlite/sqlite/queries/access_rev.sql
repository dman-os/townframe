-- name: access_rev :optional
-- param: part_key Vec<u8>
-- param: principal Vec<u8>
-- returns: rev i64
SELECT rev
  FROM big_automata_access
 WHERE part_key = :part_key
   AND principal = :principal
