-- name: meta_rev :one
-- returns: rev i64
SELECT rev
  FROM big_automata_meta
 WHERE id = 1
