-- name: route_readable_object_rows :many
-- param: principal Vec<u8>
-- param: obj_key Vec<u8>
-- param: lo i64
-- param: hi i64
-- param: limit i64
-- returns: part_key Vec<u8>, rev i64, live i64, added_at i64
SELECT m.part_key AS part_key
     , m.rev AS rev
     , m.live AS live
     , m.added_at AS added_at
  FROM big_automata_members AS m
  JOIN big_automata_access AS a
    ON a.part_key = m.part_key
   AND a.principal = :principal
   AND a.level IS NOT NULL
 WHERE m.obj_key = :obj_key
   AND m.rev > :lo
   AND m.rev <= :hi
 ORDER BY m.rev, m.part_key
 LIMIT :limit
