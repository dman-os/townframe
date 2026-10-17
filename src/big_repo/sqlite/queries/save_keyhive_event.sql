-- name: save_keyhive_event :exec
-- param: scope_id i64
-- param: seq i64
-- param: event_hash Vec<u8>
-- param: event_bytes Vec<u8>
-- param: source_id Option<Vec<u8>>
INSERT INTO big_repo_keyhive_event_log (
       scope_id
     , seq
     , event_hash
     , event_bytes
     , source_id
)
VALUES (:scope_id, :seq, :event_hash, :event_bytes, :source_id)
ON CONFLICT(scope_id, event_hash) DO NOTHING;
