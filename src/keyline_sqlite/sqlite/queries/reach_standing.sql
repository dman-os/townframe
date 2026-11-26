-- name: reach_standing :one
-- param: s Vec<u8>
-- param: n Vec<u8>
-- returns: lvl Option<i64>
SELECT MAX(lvl) AS lvl
  FROM keyline_reach
 WHERE s = :s
   AND n = :n
