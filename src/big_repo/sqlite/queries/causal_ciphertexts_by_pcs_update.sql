-- name: causal_ciphertexts_by_pcs_update :many
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: pcs_update_hash Vec<u8>
-- returns: blob Vec<u8>
SELECT commits.blob
  FROM big_repo_causal_ciphertext_index AS causal
  JOIN big_repo_subduction_commits AS commits
    ON commits.scope_id = causal.scope_id
   AND commits.sedimentree_id = causal.sedimentree_id
   AND commits.commit_id = causal.content_ref AND commits.digest = causal.digest
 WHERE causal.scope_id = :scope_id AND causal.sedimentree_id = :sedimentree_id
   AND causal.pcs_update_hash = :pcs_update_hash AND causal.kind = 0
 UNION ALL
SELECT fragments.blob
  FROM big_repo_causal_ciphertext_index AS causal
  JOIN big_repo_subduction_fragments AS fragments
    ON fragments.scope_id = causal.scope_id
   AND fragments.sedimentree_id = causal.sedimentree_id
   AND fragments.head_id = causal.content_ref AND fragments.digest = causal.digest
 WHERE causal.scope_id = :scope_id AND causal.sedimentree_id = :sedimentree_id
   AND causal.pcs_update_hash = :pcs_update_hash AND causal.kind = 1;
