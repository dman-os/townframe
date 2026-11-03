-- name: advance :exec
-- param: source_revision i64
UPDATE big_blobs_projection_progress
   SET source_revision = MAX(source_revision, :source_revision)
 WHERE singleton = 1;
