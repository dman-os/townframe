-- name: load_keyhive_events :many
-- param: scope_id i64
-- returns: event_hash Vec<u8>, event_bytes Vec<u8>, source_id Option<Vec<u8>>
-- into: crate::store::sqlite::KeyhiveEventQueryRow
SELECT event_hash
     , event_bytes
     , source_id
  FROM big_repo_keyhive_event_log
 WHERE scope_id = :scope_id
 ORDER BY seq;
