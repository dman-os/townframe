-- name: part_exists :optional
-- param: scope_id i64
-- param: part_id Vec<u8>
-- returns: present i64
SELECT 1 AS present FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id;
