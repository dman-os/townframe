-- name: live_standing :one
-- param: s Vec<u8>
-- param: ctx i64
-- param: n Vec<u8>
-- returns: lvl Option<i64>
SELECT MAX(lvl) AS lvl
  FROM keyline_live
 WHERE s = :s
   AND ctx = :ctx
   AND n = :n
