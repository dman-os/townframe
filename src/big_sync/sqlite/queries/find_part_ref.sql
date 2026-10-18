-- name: find_part_ref :optional
-- param: scope_id i64
-- param: part_id Vec<u8>
-- returns: part_ref i64
SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id
