-- name: walker_progress :optional
-- param: namespace String
-- param: consumer_id String
-- returns: upstream_revision i64
SELECT upstream_revision
  FROM delta_walker_progress
 WHERE namespace = :namespace AND consumer_id = :consumer_id
