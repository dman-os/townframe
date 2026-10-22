-- name: upsert_member_event :exec
-- param: scope_id i64
-- param: obj_ref i64
-- param: maybe_part_ref i64
-- param: event_type i64
-- param: txid i64
-- param: removed i64
INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid, added_at)
VALUES (:scope_id, :obj_ref, :maybe_part_ref, :event_type, :txid, :txid)
ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
    event_type = excluded.event_type
  , txid = excluded.txid
  , added_at = CASE
        WHEN big_sync_members.event_type = :removed THEN excluded.txid
        ELSE big_sync_members.added_at
    END
