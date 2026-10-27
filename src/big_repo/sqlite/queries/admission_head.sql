-- name: admission_head :one
-- param: scope_id i64
-- returns: head i64
SELECT COALESCE(MAX(seq), 0) AS head
  FROM big_repo_keyhive_admissions
 WHERE scope_id = :scope_id;
