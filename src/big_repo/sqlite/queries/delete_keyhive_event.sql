-- name: delete_keyhive_event :exec
-- param: scope_id i64
-- param: event_hash Vec<u8>
INSERT OR IGNORE INTO big_repo_keyhive_event_tombstones (scope_id, event_hash)
VALUES (:scope_id, :event_hash);
