-- name: commit_blobs_by_pcs_update :many
-- param: tree_id Vec<u8>
-- param: pcs_update_hash Vec<u8>
-- returns: blob Vec<u8>
SELECT blob
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
   AND pcs_update_hash = :pcs_update_hash
