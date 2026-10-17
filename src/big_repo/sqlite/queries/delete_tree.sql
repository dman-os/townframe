-- name: delete_tree :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
DELETE FROM big_repo_subduction_trees WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id;
