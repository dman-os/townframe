-- name: list_fragment_ids :many
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- returns: head_id Vec<u8>
SELECT DISTINCT head_id FROM big_repo_subduction_fragments WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id ORDER BY head_id;
