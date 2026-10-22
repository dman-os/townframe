-- name: get_summary :optional
-- param: id i64
-- returns: id i64, name String
-- into: crate::gen_tests::ItemSummary
SELECT id
     , name
  FROM items
 WHERE id = :id;
