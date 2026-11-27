-- name: live_members :many
-- param: s Vec<u8>
-- returns: n Vec<u8>, lvl i64
SELECT n
     , MAX(lvl) AS lvl
  FROM keyline_live
 WHERE s = :s
   AND ctx = 0
 GROUP BY n
