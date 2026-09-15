CREATE TABLE pauperfuse_tree_format (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
  , encoding TEXT NOT NULL
) STRICT;

INSERT INTO pauperfuse_tree_format (singleton, encoding)
VALUES (1, 'unix-bytes-v1');

CREATE TABLE pauperfuse_backend (
    backend_id INTEGER PRIMARY KEY
  , name TEXT NOT NULL UNIQUE CHECK (length(name) > 0)
  , generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0)
) STRICT;

-- Source backend is a registry reference; output and version belong to that source.
CREATE TABLE pauperfuse_observed_entry (
    backend_id INTEGER NOT NULL REFERENCES pauperfuse_backend (backend_id) ON DELETE CASCADE
  , path BLOB NOT NULL
  , kind INTEGER NOT NULL
  , source_backend_id INTEGER REFERENCES pauperfuse_backend (backend_id)
  , source_output BLOB
  , source_version BLOB
  , size INTEGER CHECK (size >= 0)
  , target BLOB
  , PRIMARY KEY (backend_id, path)
  , CHECK (
        (kind = 0 AND source_backend_id IS NOT NULL AND source_output IS NOT NULL
                  AND source_version IS NOT NULL AND target IS NULL)
     OR (kind = 1 AND source_backend_id IS NULL AND source_output IS NULL
                  AND source_version IS NULL AND size IS NULL AND target IS NULL)
     OR (kind = 2 AND source_backend_id IS NULL AND source_output IS NULL
                  AND source_version IS NULL AND size IS NULL AND target IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;
