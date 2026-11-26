-- name: commit_metas :many
-- param: tree_id Vec<u8>
-- returns: signed Vec<u8>
SELECT signed
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
