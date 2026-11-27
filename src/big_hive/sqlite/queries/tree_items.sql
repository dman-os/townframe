-- name: tree_items :many
-- param: tree_id Vec<u8>
-- returns: kind i64, head Vec<u8>, digest Vec<u8>, signed Vec<u8>
SELECT 0 AS kind
     , commit_id AS head
     , digest
     , signed
  FROM sedimentree_commits
 WHERE tree_id = :tree_id
 UNION ALL
SELECT 1 AS kind
     , fragment_head AS head
     , digest
     , signed
  FROM sedimentree_fragments
 WHERE tree_id = :tree_id
