-- name: next_keyhive_event_seq :one
-- param: scope_id i64
-- returns: seq i64
SELECT COALESCE(MAX(seq), 0) + 1 AS seq
  FROM big_repo_keyhive_event_log
 WHERE scope_id = :scope_id;
