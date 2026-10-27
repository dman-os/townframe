-- name: commit_meta_rows :many
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- returns: signed Vec<u8>
SELECT signed FROM big_repo_subduction_commits WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id ORDER BY commit_id, digest;
