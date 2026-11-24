-- name: all_access_rows :many
-- param: lo i64
-- param: hi i64
-- param: limit i64
-- returns: part_key Vec<u8>, principal Vec<u8>, level Option<i64>, rev i64
SELECT part_key
     , principal
     , level
     , rev
  FROM big_automata_access
 WHERE rev > :lo
   AND rev <= :hi
 ORDER BY rev, part_key, principal
 LIMIT :limit
