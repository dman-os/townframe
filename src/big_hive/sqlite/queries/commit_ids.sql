-- name: commit_ids :many
-- param: tree_id Vec<u8>
-- returns: commit_id Vec<u8>
SELECT commit_id
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
