-- name: namespace_state :optional
-- param: name String
-- returns: namespace_id i64, revision i64
SELECT namespace_id
     , revision
  FROM wflow_atomic_namespaces
 WHERE name = :name
