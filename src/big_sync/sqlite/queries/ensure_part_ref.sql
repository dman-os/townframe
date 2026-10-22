-- name: ensure_part_ref :one
-- param: scope_id i64
-- param: part_id Vec<u8>
-- returns: part_ref i64
INSERT INTO big_sync_parts(scope_id, part_id)
VALUES (:scope_id, :part_id)
ON CONFLICT(scope_id, part_id) DO UPDATE SET part_id = excluded.part_id
RETURNING part_ref
