-- name: source_revision :many
-- returns: source_revision i64
SELECT source_revision
  FROM big_blobs_projection_progress
 WHERE singleton = 1;
