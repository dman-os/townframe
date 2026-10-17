-- name: delete_all_fragments :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
DELETE FROM big_repo_subduction_fragments WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id;
