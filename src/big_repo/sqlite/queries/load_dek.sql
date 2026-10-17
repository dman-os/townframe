-- name: load_dek :optional
-- param: scope_id i64
-- param: dek_id String
-- param: dek_version i64
-- returns: wrapped_dek Vec<u8>, kek_version i64, algorithm String
SELECT wrapped_dek
     , kek_version
     , algorithm
  FROM big_repo_deks
 WHERE scope_id = :scope_id AND dek_id = :dek_id AND dek_version = :dek_version;
