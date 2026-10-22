-- name: register_keyhive_admission_reader :exec
-- param: scope_id i64
-- param: reader String
INSERT INTO big_repo_keyhive_admission_readers (scope_id, reader)
VALUES (:scope_id, :reader)
ON CONFLICT(scope_id, reader) DO NOTHING;
