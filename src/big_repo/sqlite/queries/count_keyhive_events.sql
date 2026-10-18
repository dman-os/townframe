-- name: count_keyhive_events :one
-- param: scope_id i64
-- returns: count i64
SELECT COUNT(*) AS count FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id;
