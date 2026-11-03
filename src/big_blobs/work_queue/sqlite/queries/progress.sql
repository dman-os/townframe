-- name: progress :many
-- param: source String
-- returns: revision i64
SELECT revision
  FROM big_blobs_progress
 WHERE source = :source;
