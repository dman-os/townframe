-- name: dispute_of :optional
-- param: delegation Vec<u8>
-- returns: ctx i64, total i64
SELECT ctx
     , total
  FROM keyline_dispute
 WHERE delegation = :delegation
