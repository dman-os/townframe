-- name: namespace_revision :one
-- param: namespace_id i64
-- returns: revision i64
SELECT revision
  FROM wflow_atomic_namespaces
 WHERE namespace_id = :namespace_id
