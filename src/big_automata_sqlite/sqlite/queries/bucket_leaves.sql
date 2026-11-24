-- name: bucket_leaves :many
-- param: part_key Vec<u8>
-- param: start i64
-- param: end_exclusive i64
-- param: after_buck i64
-- param: after_key Vec<u8>
-- param: limit i64
-- returns: obj_key Vec<u8>, buck i64, live i64, digest Option<Vec<u8>>
SELECT m.obj_key AS obj_key
     , m.buck AS buck
     , m.live AS live
     , o.digest AS digest
  FROM big_automata_members AS m
  LEFT JOIN big_automata_objs AS o ON o.obj_key = m.obj_key
 WHERE m.part_key = :part_key
   AND m.buck >= :start
   AND m.buck < :end_exclusive
   AND (m.buck, m.obj_key) > (:after_buck, :after_key)
 ORDER BY m.buck, m.obj_key
 LIMIT :limit
