-- name: all_member_rows :many
-- param: lo i64
-- param: hi i64
-- param: limit i64
-- returns: obj_key Vec<u8>, part_key Vec<u8>, rev i64, live i64, added_at i64
SELECT obj_key
     , part_key
     , rev
     , live
     , added_at
  FROM big_automata_members
 WHERE rev > :lo
   AND rev <= :hi
 ORDER BY rev, obj_key, part_key
 LIMIT :limit
