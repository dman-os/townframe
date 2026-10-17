-- name: walker_advance_progress :exec
-- param: next i64
-- param: namespace String
-- param: consumer_id String
-- param: expected i64
UPDATE delta_walker_progress
   SET upstream_revision = :next
 WHERE namespace = :namespace
   AND consumer_id = :consumer_id
   AND upstream_revision = :expected
