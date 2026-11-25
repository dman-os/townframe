-- name: set_meta_rev :exec
-- param: rev i64
UPDATE big_automata_meta
   SET rev = :rev
 WHERE id = 1
