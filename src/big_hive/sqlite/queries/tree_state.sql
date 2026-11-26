-- name: tree_state :optional
-- param: tree_id Vec<u8>
-- returns: rev i64, live bool
SELECT rev
     , live
  FROM sedimentree_trees
 WHERE tree_id = :tree_id
