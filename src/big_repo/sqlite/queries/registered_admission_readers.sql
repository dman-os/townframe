-- name: registered_admission_readers :many
-- param: scope_id i64
-- returns: reader String
SELECT reader FROM big_repo_keyhive_admission_readers WHERE scope_id = :scope_id;
