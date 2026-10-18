-- name: contains_sedimentree_id :optional
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- returns: found i64
SELECT 1 AS found FROM big_repo_subduction_trees WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id;
