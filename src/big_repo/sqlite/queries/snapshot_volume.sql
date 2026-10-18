-- name: snapshot_volume :one
-- param: scope_id i64
-- returns: kh_count i64, kh_bytes i64, sediment_count i64, sediment_bytes i64
SELECT (SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id) AS kh_count
     , (SELECT COALESCE(SUM(length(event_bytes)), 0) FROM big_repo_keyhive_event_log WHERE scope_id = :scope_id) AS kh_bytes
     , ((SELECT COUNT(*) FROM big_repo_subduction_commits WHERE scope_id = :scope_id) +
        (SELECT COUNT(*) FROM big_repo_subduction_fragments WHERE scope_id = :scope_id)) AS sediment_count
     , ((SELECT COALESCE(SUM(length(blob)), 0) FROM big_repo_subduction_commits WHERE scope_id = :scope_id) +
        (SELECT COALESCE(SUM(length(blob)), 0) FROM big_repo_subduction_fragments WHERE scope_id = :scope_id)) AS sediment_bytes;
