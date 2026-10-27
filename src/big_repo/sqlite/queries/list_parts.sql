-- name: list_parts :many
-- param: scope_id i64
-- returns: part_id Vec<u8>
SELECT part_id FROM big_sync_parts WHERE scope_id = :scope_id;
