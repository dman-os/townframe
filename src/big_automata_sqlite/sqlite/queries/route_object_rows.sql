-- name: route_object_rows :many
-- param: obj_key Vec<u8>
-- param: lo i64
-- param: hi i64
-- param: limit i64
-- returns: part_key Vec<u8>, rev i64, live i64, added_at i64
SELECT part_key
     , rev
     , live
     , added_at
  FROM big_automata_members
 WHERE obj_key = :obj_key
   AND rev > :lo
   AND rev <= :hi
 ORDER BY rev, part_key
 LIMIT :limit
