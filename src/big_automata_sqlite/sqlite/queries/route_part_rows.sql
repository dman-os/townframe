-- name: route_part_rows :many
-- param: part_key Vec<u8>
-- param: lo i64
-- param: hi i64
-- param: limit i64
-- returns: obj_key Vec<u8>, rev i64, live i64, added_at i64
SELECT obj_key
     , rev
     , live
     , added_at
  FROM big_automata_members
 WHERE part_key = :part_key
   AND rev > :lo
   AND rev <= :hi
 ORDER BY rev, obj_key
 LIMIT :limit
