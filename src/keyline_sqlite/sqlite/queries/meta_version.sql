-- name: meta_version :one
-- returns: version i64
SELECT version
  FROM keyline_meta
 WHERE id = 1
