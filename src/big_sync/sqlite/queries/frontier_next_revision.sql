-- name: frontier_next_revision :one
-- param: frontier_id String
-- returns: revision i64
UPDATE generic_frontier_meta
   SET revision = revision + 1
 WHERE frontier_id = :frontier_id
RETURNING revision
