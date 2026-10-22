-- name: admission_events_after :many
-- param: scope_id i64
-- param: cursor i64
-- param: limit i64
-- returns: seq i64, event_hash Vec<u8>, source_id Option<Vec<u8>>, event_bytes Vec<u8>
SELECT a.seq
     , a.event_hash
     , a.source_id
     , e.event_bytes
  FROM big_repo_keyhive_admissions a
  JOIN big_repo_keyhive_event_log e ON e.scope_id = a.scope_id AND e.event_hash = a.event_hash
 WHERE a.scope_id = :scope_id AND a.seq > :cursor
 ORDER BY a.seq
 LIMIT :limit;
