-- name: commit_rows :many
-- param: tree_id Vec<u8>
-- returns: signed Vec<u8>, blob Vec<u8>
SELECT signed
     , blob
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
