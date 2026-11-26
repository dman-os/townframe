-- name: register_tree :optional
-- param: tree_id Vec<u8>
-- param: rev Option<i64>
-- returns: rev i64
INSERT INTO sedimentree_trees (tree_id, rev, live)
VALUES (:tree_id, COALESCE(:rev, (SELECT COALESCE(MAX(rev), 0) + 1 FROM sedimentree_trees)), 1)
    ON CONFLICT (tree_id) DO UPDATE
   SET rev = excluded.rev
     , live = 1
 WHERE sedimentree_trees.live = 0
RETURNING rev
