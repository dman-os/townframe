-- name: reach_members :many
-- param: s Vec<u8>
-- returns: n Vec<u8>, lvl i64
SELECT n
     , MAX(lvl) AS lvl
  FROM keyline_reach
 WHERE s = :s
 GROUP BY n
