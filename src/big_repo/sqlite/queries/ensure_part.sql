-- name: ensure_part :exec
-- param: scope_id i64
-- param: part_id Vec<u8>
INSERT INTO big_sync_parts (scope_id, part_id, latest_cursor)
VALUES (:scope_id, :part_id, 0)
ON CONFLICT(scope_id, part_id) DO NOTHING;
