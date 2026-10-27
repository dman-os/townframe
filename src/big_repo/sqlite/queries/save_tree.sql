-- name: save_tree :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
INSERT OR IGNORE INTO big_repo_subduction_trees (scope_id, sedimentree_id) VALUES (:scope_id, :sedimentree_id);
