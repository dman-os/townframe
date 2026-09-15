EXPLAIN QUERY PLAN
INSERT INTO pauperfuse_observed_entry (
    backend_id
  , path
  , kind
  , source_backend_id
  , source_output
  , source_version
  , size
  , target
)
SELECT backend_id
     , path
     , kind
     , source_backend_id
     , source_output
     , source_version
     , size
     , target
  FROM pauperfuse_tree_conversion;
