-- name: save_dek :exec
-- param: scope_id i64
-- param: dek_id String
-- param: dek_version i64
-- param: wrapped_dek Vec<u8>
-- param: kek_version i64
-- param: algorithm String
INSERT INTO big_repo_deks (
       scope_id
     , dek_id
     , dek_version
     , wrapped_dek
     , kek_version
     , algorithm
)
VALUES (:scope_id, :dek_id, :dek_version, :wrapped_dek, :kek_version, :algorithm)
ON CONFLICT(scope_id, dek_id, dek_version) DO UPDATE SET
       wrapped_dek = excluded.wrapped_dek
     , kek_version = excluded.kek_version
     , algorithm = excluded.algorithm;
