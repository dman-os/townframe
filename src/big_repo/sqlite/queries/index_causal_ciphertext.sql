-- name: index_causal_ciphertext :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: content_ref Vec<u8>
-- param: digest Vec<u8>
-- param: kind i64
-- param: pcs_update_hash Vec<u8>
INSERT OR IGNORE INTO big_repo_causal_ciphertext_index (
       scope_id
     , sedimentree_id
     , content_ref
     , digest
     , kind
     , pcs_update_hash
)
VALUES (:scope_id, :sedimentree_id, :content_ref, :digest, :kind, :pcs_update_hash);
