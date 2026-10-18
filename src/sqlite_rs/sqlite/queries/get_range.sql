-- name: get_range :optional
-- param: id i64
-- returns: start i64, end i64
-- into: crate::gen_tests::ExternalRange
SELECT id AS start
     , id + 1 AS end
  FROM items
 WHERE id = :id;
