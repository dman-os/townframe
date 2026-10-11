EXPLAIN QUERY PLAN
-- Bind: 1 backend_id, 2 limit.
SELECT entry.path
     , entry.kind
     , source.name AS source_name
     , entry.source_output
     , entry.source_version
     , entry.size
     , entry.target
  FROM pauperfuse_observed_entry AS entry
  LEFT JOIN pauperfuse_backend AS source ON source.backend_id = entry.source_backend_id
 WHERE entry.backend_id = ?
 ORDER BY entry.path
 LIMIT ?;
