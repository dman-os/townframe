-- name: advance_revision :optional
-- param: namespace_id i64
-- param: expected_revision i64
-- returns: revision i64
UPDATE wflow_atomic_namespaces
   SET revision = revision + 1
 WHERE namespace_id = :namespace_id
   AND revision = :expected_revision
RETURNING revision
