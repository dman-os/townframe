-- name: insert_dead_member :exec
-- param: scope_id i64
-- param: obj_ref i64
-- param: part_ref i64
-- param: removed i64
-- param: cursor i64
INSERT INTO big_sync_members (scope_id, obj_ref, maybe_part_ref, event_type, txid)
VALUES (:scope_id, :obj_ref, :part_ref, :removed, :cursor)
ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
       event_type = excluded.event_type
     , txid = excluded.txid;
