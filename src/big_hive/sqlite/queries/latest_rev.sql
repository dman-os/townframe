-- name: latest_rev :one
-- returns: rev i64
SELECT COALESCE(MAX(rev), 0) AS rev
  FROM sedimentree_trees
