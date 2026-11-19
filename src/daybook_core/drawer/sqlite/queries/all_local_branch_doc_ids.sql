-- name: all_local_branch_doc_ids :many
-- returns: branch_doc_id Vec<u8>
SELECT branch_doc_id FROM drawer_local_branches
