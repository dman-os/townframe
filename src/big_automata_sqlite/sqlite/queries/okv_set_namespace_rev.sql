-- name: okv_set_namespace_rev :exec
-- param: rev i64
-- param: namespace_id i64
UPDATE big_automata_okv_namespaces
   SET rev = :rev
 WHERE namespace_id = :namespace_id
