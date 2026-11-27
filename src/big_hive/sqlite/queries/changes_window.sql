-- name: changes_window :many
-- param: after i64
-- param: until i64
-- param: limit i64
-- returns: tree_id Vec<u8>, rev i64, live bool
SELECT tree_id
     , rev
     , live
  FROM sedimentree_trees
 WHERE rev > :after
   AND rev <= :until
 ORDER BY rev
        , tree_id
 LIMIT :limit
