-- name: load_all_sedimentree_ids :many
-- param: scope_id i64
-- returns: sedimentree_id Vec<u8>
SELECT sedimentree_id FROM big_repo_subduction_trees WHERE scope_id = :scope_id ORDER BY sedimentree_id;
