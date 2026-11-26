-- name: bump_version :exec
UPDATE keyline_meta
   SET version = version + 1
 WHERE id = 1
