-- name: pair_roots_unresolved :many
-- returns: cipher_hash String, doc_id Option<String>, branch_path Option<String>
SELECT cipher_hash, doc_id, branch_path FROM blob_pair_root
