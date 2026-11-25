-- name: okv_namespace_by_name :one
-- param: name String
-- returns: namespace_id i64, rev i64
SELECT namespace_id
     , rev
  FROM big_automata_okv_namespaces
 WHERE name = :name
