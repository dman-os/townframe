-- name: archived_through :one
-- param: scope_id i64
-- returns: seq i64
SELECT COALESCE(MAX(seq), 0) AS seq
  FROM big_repo_keyhive_archived_through
 WHERE scope_id = :scope_id;
