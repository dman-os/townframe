-- name: frontier_revision :one
-- param: frontier_id String
-- returns: revision i64
SELECT revision
  FROM generic_frontier_meta
 WHERE frontier_id = :frontier_id
