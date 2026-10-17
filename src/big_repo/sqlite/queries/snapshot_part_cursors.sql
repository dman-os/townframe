-- name: snapshot_part_cursors :many
-- param: scope_id i64
-- returns: part_id Vec<u8>, latest_cursor i64
SELECT part_id
     , latest_cursor
  FROM big_sync_parts WHERE scope_id = :scope_id ORDER BY part_id;
