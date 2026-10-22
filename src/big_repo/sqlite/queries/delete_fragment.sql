-- name: delete_fragment :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: head_id Vec<u8>
DELETE FROM big_repo_subduction_fragments WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id AND head_id = :head_id;
