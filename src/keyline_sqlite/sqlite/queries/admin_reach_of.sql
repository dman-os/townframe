-- name: admin_reach_of :many
-- param: k Vec<u8>
-- returns: node Vec<u8>
-- The nodes over which `k` has held Admin: with `k` itself, its admin reach.
SELECT s AS node
  FROM keyline_reach
 WHERE n = :k
   AND lvl = 3
