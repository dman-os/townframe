CREATE TABLE wflow_atomic_namespaces (
    namespace_id INTEGER PRIMARY KEY
  , name TEXT NOT NULL UNIQUE
  , revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;

CREATE TABLE wflow_atomic_entries (
    namespace_id INTEGER NOT NULL REFERENCES wflow_atomic_namespaces(namespace_id)
  , key BLOB NOT NULL
  , value BLOB NOT NULL
  , PRIMARY KEY (namespace_id, key)
) STRICT;
