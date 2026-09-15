EXPLAIN QUERY PLAN
-- Bind: 1 backend_id, 2 path, 3 kind, 4 source_backend_id, 5 output, 6 version, 7 size, 8 target.
INSERT INTO pauperfuse_observed_entry (
    backend_id
  , path
  , kind
  , source_backend_id
  , source_output
  , source_version
  , size
  , target
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
