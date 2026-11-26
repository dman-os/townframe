-- name: touch_tree :one
-- param: tree_id Vec<u8>
-- returns: rev i64
INSERT INTO sedimentree_trees (tree_id, rev, live)
VALUES (:tree_id, (SELECT COALESCE(MAX(rev), 0) + 1 FROM sedimentree_trees), 1)
    ON CONFLICT (tree_id) DO UPDATE
   SET rev = excluded.rev
     , live = 1
RETURNING rev
