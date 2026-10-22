-- name: obj_exists :optional
-- param: scope_id i64
-- param: obj_id Vec<u8>
-- returns: present i64
SELECT 1 AS present
  FROM big_sync_objs
 WHERE scope_id = :scope_id AND obj_id = :obj_id
