EXPLAIN QUERY PLAN
CREATE TEMP TABLE pauperfuse_tree_conversion (
    backend_id INTEGER NOT NULL
  , path BLOB NOT NULL
  , kind INTEGER NOT NULL
  , source_backend_id INTEGER
  , source_output BLOB
  , source_version BLOB
  , size INTEGER
  , target BLOB
  , PRIMARY KEY (backend_id, path)
) STRICT, WITHOUT ROWID;
