EXPLAIN QUERY PLAN
-- Bind: 1 backend_id.
SELECT generation
  FROM pauperfuse_backend
 WHERE backend_id = ?;
