-- name: commit_row :optional
-- param: tree_id Vec<u8>
-- param: commit_id Vec<u8>
-- returns: signed Vec<u8>, blob Vec<u8>
SELECT signed
     , blob
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
   AND commit_id = :commit_id
