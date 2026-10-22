-- name: get_peer_part_cursor :optional
-- param: scope_id i64
-- param: peer_id Vec<u8>
-- param: part_id Vec<u8>
-- returns: cursor i64
SELECT cursor FROM big_sync_peer_cursors
 WHERE scope_id = :scope_id AND peer_id = :peer_id
   AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id);
