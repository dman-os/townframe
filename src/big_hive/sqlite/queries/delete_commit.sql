-- name: delete_commit :exec
-- param: tree_id Vec<u8>
-- param: commit_id Vec<u8>
DELETE FROM sedimentree_commits
 WHERE tree_id = :tree_id
   AND commit_id = :commit_id
