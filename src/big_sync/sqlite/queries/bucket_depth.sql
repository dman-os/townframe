-- name: bucket_depth :one
-- returns: value i64
SELECT value
  FROM big_sync_meta
 WHERE key = 'bucket_depth'
