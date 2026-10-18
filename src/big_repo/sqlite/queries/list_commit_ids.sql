-- name: list_commit_ids :many
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- returns: commit_id Vec<u8>
SELECT DISTINCT commit_id FROM big_repo_subduction_commits WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id ORDER BY commit_id;
