-- name: touch_member :exec
-- param: event_type i64
-- param: txid i64
-- param: scope_id i64
-- param: obj_ref i64
-- param: part_ref i64
UPDATE big_sync_members
   SET event_type = :event_type
     , txid = :txid
 WHERE scope_id = :scope_id AND obj_ref = :obj_ref AND maybe_part_ref = :part_ref;
