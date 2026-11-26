-- name: delete_commits :exec
-- param: tree_id Vec<u8>
DELETE FROM sedimentree_commits
 WHERE tree_id = :tree_id
