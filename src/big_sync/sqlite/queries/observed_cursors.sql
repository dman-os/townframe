-- name: observed_cursors :many
-- param: scope_id i64
-- returns: peer_id Vec<u8>, part_id Vec<u8>, cursor i64
SELECT peer_id
     , parts.part_id
     , cursor
  FROM big_sync_peer_cursors
  JOIN big_sync_parts parts ON parts.part_ref = big_sync_peer_cursors.part_ref
 WHERE big_sync_peer_cursors.scope_id = :scope_id
