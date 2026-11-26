-- name: total_op_count :optional
-- returns: ops i64
SELECT ops
  FROM kh_op_count
 WHERE one = 1
