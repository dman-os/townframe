-- name: count_loose_ciphertexts :one
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- returns: count i64
SELECT COUNT(*) AS count FROM big_repo_causal_ciphertext_index
 WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id AND kind = 0;
