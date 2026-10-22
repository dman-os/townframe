-- name: keyhive_event_log_cursor :one
-- param: scope_id i64
-- returns: seq Option<i64>
SELECT MAX(seq) AS seq FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id;
