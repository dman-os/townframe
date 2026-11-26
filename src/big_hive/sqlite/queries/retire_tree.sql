-- name: retire_tree :optional
-- param: rev Option<i64>
-- param: tree_id Vec<u8>
-- returns: rev i64
UPDATE sedimentree_trees
   SET rev = COALESCE(:rev, (SELECT MAX(rev) + 1 FROM sedimentree_trees))
     , live = 0
 WHERE tree_id = :tree_id
   AND live = 1
RETURNING rev
