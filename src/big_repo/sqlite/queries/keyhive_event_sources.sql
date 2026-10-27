-- name: keyhive_event_sources :many
-- param: scope_id i64
-- returns: event_hash Vec<u8>, source_id Option<Vec<u8>>
SELECT event_hash
     , source_id
  FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id;
