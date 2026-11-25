-- name: okv_namespace_rev :one
-- param: namespace_id i64
-- returns: rev i64
SELECT rev
  FROM big_automata_okv_namespaces
 WHERE namespace_id = :namespace_id
