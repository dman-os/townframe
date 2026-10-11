EXPLAIN QUERY PLAN
-- Bind: 1 encoding.
UPDATE pauperfuse_tree_format
   SET encoding = ?
 WHERE singleton = 1;
