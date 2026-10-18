-- name: keyhive_event_counts :one
-- param: scope_id i64
-- returns: logged i64, admitted i64, head i64
SELECT (SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id) AS logged
     , (SELECT COUNT(*) FROM big_repo_keyhive_admissions WHERE scope_id = :scope_id) AS admitted
     , (SELECT COALESCE(MAX(seq), 0) FROM big_repo_keyhive_admissions WHERE scope_id = :scope_id) AS head;
