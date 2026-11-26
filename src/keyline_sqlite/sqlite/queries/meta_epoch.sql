-- name: meta_epoch :one
-- returns: epoch i64
SELECT epoch
  FROM keyline_meta
 WHERE id = 1
