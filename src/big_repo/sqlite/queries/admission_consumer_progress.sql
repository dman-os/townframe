-- name: admission_consumer_progress :one
-- param: namespace String
-- param: consumer_id String
-- returns: revision i64
SELECT COALESCE(MAX(upstream_revision), 0) AS revision
  FROM delta_walker_progress
 WHERE namespace = :namespace AND consumer_id = :consumer_id;
