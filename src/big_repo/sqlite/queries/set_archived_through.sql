-- name: set_archived_through :exec
-- param: scope_id i64
-- param: seq i64
INSERT INTO big_repo_keyhive_archived_through (scope_id, seq)
VALUES (:scope_id, :seq)
ON CONFLICT(scope_id) DO UPDATE SET seq = MAX(seq, excluded.seq);
