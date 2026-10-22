CREATE TABLE IF NOT EXISTS generic_frontier_meta (
    frontier_id TEXT PRIMARY KEY
  , revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS generic_frontier_entries (
    frontier_id TEXT NOT NULL
  , namespace BLOB
  , key BLOB NOT NULL
  , revision INTEGER NOT NULL
  , value BLOB
  , PRIMARY KEY(frontier_id, key)
);
CREATE INDEX IF NOT EXISTS idx_generic_frontier_revision
    ON generic_frontier_entries (frontier_id, revision);
CREATE INDEX IF NOT EXISTS idx_generic_frontier_namespace_revision
    ON generic_frontier_entries (frontier_id, namespace, revision);

CREATE TABLE IF NOT EXISTS delta_walker_progress (
    namespace TEXT NOT NULL
  , consumer_id TEXT NOT NULL
  , upstream_revision INTEGER NOT NULL
  , PRIMARY KEY(namespace, consumer_id)
) STRICT;
CREATE TABLE IF NOT EXISTS delta_walker_key_state (
    namespace TEXT NOT NULL
  , consumer_id TEXT NOT NULL
  , state_key BLOB NOT NULL
  , state_value BLOB NOT NULL
  , PRIMARY KEY(namespace, consumer_id, state_key)
) STRICT;
