-- name: changes_after :many
-- param: after i64
-- param: limit i64
-- returns: tree_id Vec<u8>, rev i64, live bool
SELECT tree_id
     , rev
     , live
  FROM sedimentree_trees
 WHERE rev > :after
 ORDER BY rev
 LIMIT :limit
