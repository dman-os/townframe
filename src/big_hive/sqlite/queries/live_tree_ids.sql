-- name: live_tree_ids :many
-- returns: tree_id Vec<u8>
SELECT tree_id
  FROM sedimentree_trees
 WHERE live = 1
