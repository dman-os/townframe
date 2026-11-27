-- name: count_affected :one
-- returns: affected i64
SELECT COUNT(*) AS affected
  FROM keyline_affected
