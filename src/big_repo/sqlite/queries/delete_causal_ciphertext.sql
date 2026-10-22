-- name: delete_causal_ciphertext :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: content_ref Vec<u8>
-- param: kind i64
DELETE FROM big_repo_causal_ciphertext_index WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id AND content_ref = :content_ref AND kind = :kind;
