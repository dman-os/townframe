-- name: insert_live_member :exec
-- param: scope_id i64
-- param: obj_ref i64
-- param: part_ref i64
-- param: changed i64
-- param: cursor i64
-- param: removed i64
INSERT INTO big_sync_members (scope_id, obj_ref, maybe_part_ref, event_type, txid, added_at)
VALUES (:scope_id, :obj_ref, :part_ref, :changed, :cursor, :cursor)
ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
       event_type = excluded.event_type
     , txid = excluded.txid
     , added_at = CASE WHEN big_sync_members.event_type = :removed THEN excluded.added_at ELSE big_sync_members.added_at END;
