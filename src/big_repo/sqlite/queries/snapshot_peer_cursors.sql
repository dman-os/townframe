-- name: snapshot_peer_cursors :many
-- param: scope_id i64
-- returns: peer_id Vec<u8>, part_id Vec<u8>, cursor i64
SELECT peer_id
     , p.part_id
     , cursor
  FROM big_sync_peer_cursors c
  JOIN big_sync_parts p ON p.part_ref = c.part_ref
 WHERE c.scope_id = :scope_id ORDER BY peer_id, p.part_id;
