-- name: set_peer_part_cursor :exec
-- param: scope_id i64
-- param: peer_id Vec<u8>
-- param: part_ref i64
-- param: cursor i64
INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_ref, cursor)
VALUES (:scope_id, :peer_id, :part_ref, :cursor)
ON CONFLICT(scope_id, peer_id, part_ref) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)
