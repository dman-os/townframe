-- name: unadmitted_keyhive_events :many
-- param: scope_id i64
-- returns: event_hash Vec<u8>, event_bytes Vec<u8>, source_id Option<Vec<u8>>
-- into: crate::store::sqlite::KeyhiveEventQueryRow
SELECT l.event_hash
     , l.event_bytes
     , l.source_id
  FROM big_repo_keyhive_event_log l
 WHERE l.scope_id = :scope_id
   AND NOT EXISTS (
       SELECT 1 FROM big_repo_keyhive_admissions a
        WHERE a.scope_id = l.scope_id AND a.event_hash = l.event_hash
   )
 ORDER BY l.seq;
