-- name: partless_objects :many
-- param: after_key Vec<u8>
-- param: limit i64
-- returns: obj_key Vec<u8>
SELECT obj_key
  FROM big_automata_objs
 WHERE part_count = 0
   AND obj_key > :after_key
 ORDER BY obj_key
 LIMIT :limit
